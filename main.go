package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	ical "github.com/arran4/golang-ical"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/teambition/rrule-go"
)

const (
	btnToday      = "📅 События сегодня"
	btnTomorrow   = "🌅 События завтра"
	btnWeek       = "🗓 События недели"
	btnNearest    = "🔜 Ближайшее событие"
	btnWeekendOn  = "🔔 Напоминания в выходные: вкл"
	btnWeekendOff = "🔕 Напоминания в выходные: выкл"
	btnEditEmail  = "✉️ Изменить email"
	btnEditURLs   = "🔗 Изменить iCal ссылки"
	btnR15Prefix  = "⏱ За 15 минут:"
	btnR5Prefix   = "⏱ За 5 минут:"
	btnRSPrefix   = "▶️ По началу:"
)

type UserRecord struct {
	TelegramUserID              int64    `json:"telegram_user_id"`
	Username                    string   `json:"username"`
	Email                       string   `json:"email"`
	ICalURLs                    []string `json:"ical_urls,omitempty"`
	WeekendEnabled              bool     `json:"weekend_enabled"`
	Reminder15Enabled           bool     `json:"reminder_15_enabled"`
	Reminder5Enabled            bool     `json:"reminder_5_enabled"`
	ReminderStartEnabled        bool     `json:"reminder_start_enabled"`
	ReminderSettingsInitialized bool     `json:"reminder_settings_initialized"`
}

type UsersStore struct {
	path  string
	mu    sync.RWMutex
	users map[int64]UserRecord
}

type Event struct {
	UID               string
	Title             string
	Start             time.Time
	End               time.Time
	Description       string
	Location          string
	URL               string
	Organizer         string
	OrganizerEmail    string
	Participants      []string
	ParticipantEmails []string
}

type Config struct {
	BotToken         string
	UsersFile        string
	PollEvery        time.Duration
	CacheTTL         time.Duration
	DigestTime       string
	Timezone         string
	HTTPTimeout      time.Duration
	HelpImagePath    string
	LogDir           string
	LogRotateTime    string
	LogRetentionDays int
}

type ReminderEngine struct {
	mu   sync.Mutex
	sent map[string]time.Time
}

type UserEventsCacheEntry struct {
	Events      []Event
	RefreshedAt time.Time
	Err         error
}

type UserEventsCache struct {
	mu   sync.RWMutex
	ttl  time.Duration
	data map[int64]UserEventsCacheEntry
}

type InputMode string

const (
	modeNone  InputMode = ""
	modeEmail InputMode = "email"
	modeURLs  InputMode = "urls"
)

type InputStateStore struct {
	mu    sync.Mutex
	state map[int64]InputMode
}

type DailyLogWriter struct {
	mu         sync.Mutex
	logDir     string
	file       *os.File
	currentDay string
	loc        *time.Location
	retention  int
	hour       int
	minute     int
	stopCh     chan struct{}
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		log.Fatalf("invalid timezone %q: %v", cfg.Timezone, err)
	}

	logWriter, err := NewDailyLogWriter(cfg.LogDir, cfg.LogRotateTime, cfg.LogRetentionDays, loc)
	if err != nil {
		log.Fatalf("logger init error: %v", err)
	}
	defer logWriter.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logWriter))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	store, err := NewUsersStore(cfg.UsersFile)
	if err != nil {
		log.Fatalf("users store error: %v", err)
	}

	bot, err := tgbotapi.NewBotAPI(cfg.BotToken)
	if err != nil {
		log.Fatalf("telegram bot init error: %v", err)
	}
	log.Printf("authorized as %s", bot.Self.UserName)

	httpClient := &http.Client{Timeout: cfg.HTTPTimeout}
	engine := &ReminderEngine{sent: make(map[string]time.Time)}
	eventsCache := NewUserEventsCache(cfg.CacheTTL)
	inputStates := &InputStateStore{state: make(map[int64]InputMode)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go runReminderLoop(ctx, bot, store, eventsCache, httpClient, cfg.PollEvery, loc, engine)
	go runDailyDigestLoop(ctx, bot, store, eventsCache, httpClient, cfg.DigestTime, loc)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 30
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil {
			continue
		}
		handleMessage(bot, store, eventsCache, inputStates, httpClient, loc, cfg.HelpImagePath, update.Message)
	}
}

func loadConfig() (Config, error) {
	cfg := Config{
		BotToken:         strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN")),
		UsersFile:        envOrDefault("USERS_FILE", "data/users.json"),
		PollEvery:        durationEnvOrDefault("POLL_EVERY", 5*time.Minute),
		CacheTTL:         durationEnvOrDefault("CACHE_TTL", 60*time.Second),
		DigestTime:       envOrDefault("DAILY_DIGEST_TIME", "10:00"),
		Timezone:         envOrDefault("TZ_LOCATION", "Europe/Moscow"),
		HTTPTimeout:      durationEnvOrDefault("HTTP_TIMEOUT", 20*time.Second),
		HelpImagePath:    envOrDefault("HELP_IMAGE_PATH", "help.png"),
		LogDir:           envOrDefault("LOG_DIR", "data/logs"),
		LogRotateTime:    envOrDefault("LOG_ROTATE_TIME", "00:05"),
		LogRetentionDays: intEnvOrDefault("LOG_RETENTION_DAYS", 14),
	}
	if cfg.BotToken == "" {
		return Config{}, errors.New("TELEGRAM_BOT_TOKEN is required")
	}
	if _, _, err := parseClockHHMM(cfg.DigestTime); err != nil {
		return Config{}, fmt.Errorf("invalid DAILY_DIGEST_TIME: %w", err)
	}
	if _, _, err := parseClockHHMM(cfg.LogRotateTime); err != nil {
		return Config{}, fmt.Errorf("invalid LOG_ROTATE_TIME: %w", err)
	}
	if cfg.LogRetentionDays < 1 {
		return Config{}, errors.New("LOG_RETENTION_DAYS must be >= 1")
	}
	return cfg, nil
}

func envOrDefault(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func durationEnvOrDefault(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func intEnvOrDefault(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	var out int
	if _, err := fmt.Sscanf(v, "%d", &out); err != nil {
		return def
	}
	return out
}

func parseClockHHMM(v string) (int, int, error) {
	tm, err := time.Parse("15:04", strings.TrimSpace(v))
	if err != nil {
		return 0, 0, err
	}
	return tm.Hour(), tm.Minute(), nil
}

func NewUsersStore(path string) (*UsersStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("users file path is empty")
	}
	if err := ensureParentDir(path); err != nil {
		return nil, err
	}
	s := &UsersStore{path: path, users: make(map[int64]UserRecord)}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *UsersStore) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s.flushLocked()
		}
		return err
	}
	if len(strings.TrimSpace(string(b))) == 0 {
		return nil
	}

	var list []UserRecord
	if err := json.Unmarshal(b, &list); err != nil {
		return fmt.Errorf("unmarshal users file: %w", err)
	}

	s.users = make(map[int64]UserRecord, len(list))
	for _, u := range list {
		if u.TelegramUserID == 0 {
			continue
		}
		u.ICalURLs = sanitizeURLs(u.ICalURLs)
		if !u.ReminderSettingsInitialized {
			u.Reminder15Enabled = true
			u.Reminder5Enabled = true
			u.ReminderStartEnabled = true
			u.ReminderSettingsInitialized = true
		}
		s.users[u.TelegramUserID] = u
	}
	return nil
}

func (s *UsersStore) SetCalendarURLs(userID int64, username string, urls []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	username = strings.TrimSpace(username)
	urls = sanitizeURLs(urls)
	u, ok := s.users[userID]
	if !ok {
		u = UserRecord{
			TelegramUserID:              userID,
			WeekendEnabled:              false,
			Reminder15Enabled:           true,
			Reminder5Enabled:            true,
			ReminderStartEnabled:        true,
			ReminderSettingsInitialized: true,
		}
	}
	if username == "" {
		username = u.Username
	}
	u.Username = username
	u.ICalURLs = urls
	s.users[userID] = u
	return s.flushLocked()
}

func (s *UsersStore) SetWeekendEnabled(userID int64, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[userID]
	if !ok {
		u = UserRecord{
			TelegramUserID:              userID,
			Reminder15Enabled:           true,
			Reminder5Enabled:            true,
			ReminderStartEnabled:        true,
			ReminderSettingsInitialized: true,
		}
	}
	u.WeekendEnabled = enabled
	s.users[userID] = u
	return s.flushLocked()
}

func (s *UsersStore) SetEmail(userID int64, email string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[userID]
	if !ok {
		u = UserRecord{
			TelegramUserID:              userID,
			Reminder15Enabled:           true,
			Reminder5Enabled:            true,
			ReminderStartEnabled:        true,
			ReminderSettingsInitialized: true,
		}
	}
	u.Email = normalizeEmail(email)
	s.users[userID] = u
	return s.flushLocked()
}

func (s *UsersStore) SetReminderEnabled(userID int64, kind string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[userID]
	if !ok {
		u = UserRecord{
			TelegramUserID:              userID,
			Reminder15Enabled:           true,
			Reminder5Enabled:            true,
			ReminderStartEnabled:        true,
			ReminderSettingsInitialized: true,
		}
	}
	switch kind {
	case "15m":
		u.Reminder15Enabled = enabled
	case "5m":
		u.Reminder5Enabled = enabled
	case "start":
		u.ReminderStartEnabled = enabled
	default:
		return fmt.Errorf("unknown reminder kind: %s", kind)
	}
	u.ReminderSettingsInitialized = true
	s.users[userID] = u
	return s.flushLocked()
}

func (s *UsersStore) Get(userID int64) (UserRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[userID]
	return u, ok
}

func (s *UsersStore) List() []UserRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]UserRecord, 0, len(s.users))
	for _, u := range s.users {
		out = append(out, u)
	}
	return out
}

func (s *UsersStore) flushLocked() error {
	if err := ensureParentDir(s.path); err != nil {
		return err
	}
	list := make([]UserRecord, 0, len(s.users))
	for _, u := range s.users {
		list = append(list, u)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].TelegramUserID < list[j].TelegramUserID })

	b, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, b, 0o644)
}

func ensureParentDir(filePath string) error {
	dir := strings.TrimSpace(filePath)
	if dir == "" {
		return errors.New("empty users file path")
	}
	parent := "."
	if d := strings.TrimSpace(strings.TrimSuffix(filePath, "/")); d != "" {
		parent = filepath.Dir(filePath)
	}
	if parent == "." || parent == "" {
		return nil
	}
	return os.MkdirAll(parent, 0o755)
}

func handleMessage(bot *tgbotapi.BotAPI, store *UsersStore, eventsCache *UserEventsCache, inputStates *InputStateStore, client *http.Client, loc *time.Location, helpImagePath string, m *tgbotapi.Message) {
	if m.From == nil {
		return
	}
	chatID := m.Chat.ID
	text := strings.TrimSpace(m.Text)
	userID := m.From.ID
	username := strings.TrimSpace(m.From.UserName)

	if strings.HasPrefix(text, "/start") {
		inputStates.Set(userID, modeEmail)
		log.Printf("user start: id=%d username=%q", userID, username)
		sendText(bot, chatID, "👋 Привет! Я помогу с напоминаниями из Яндекс Календаря.")
		sendText(bot, chatID, "✉️ Шаг 1/2. Отправьте ваш e-mail (тот, что участвует во встречах).")
		return
	}

	if strings.HasPrefix(text, "/setical") {
		raw := strings.TrimSpace(strings.TrimPrefix(text, "/setical"))
		urls, err := parseICalURLsInput(raw)
		if err != nil {
			sendErrorText(bot, chatID, "❌ Не удалось разобрать ссылки.", err)
			return
		}
		handleSetURLs(bot, store, eventsCache, client, loc, chatID, userID, username, urls)
		return
	}

	switch text {
	case btnEditEmail:
		inputStates.Set(userID, modeEmail)
		sendText(bot, chatID, "✉️ Отправьте email (например, user@example.com).")
		return
	case btnEditURLs:
		inputStates.Set(userID, modeURLs)
		sendText(bot, chatID, "🔗 Отправьте одну или несколько ICal ссылок.\nМожно через пробел или с новой строки.")
		return
	case btnWeekendOn, btnWeekendOff:
		currentEnabled := text == btnWeekendOn
		nextEnabled := !currentEnabled
		if err := store.SetWeekendEnabled(userID, nextEnabled); err != nil {
			sendErrorText(bot, chatID, "❌ Не удалось изменить настройку выходных.", err)
			return
		}
		if nextEnabled {
			sendText(bot, chatID, "✅ Напоминания по выходным включены.")
		} else {
			sendText(bot, chatID, "✅ Напоминания по выходным выключены.")
		}
		log.Printf("weekend reminders toggled: id=%d username=%q enabled=%t", userID, username, nextEnabled)
		sendKeyboard(bot, store, chatID, userID)
		return
	case getReminder15ButtonLabel(store, userID):
		enabled, err := toggleReminderButton(store, userID, "15m")
		if err != nil {
			sendErrorText(bot, chatID, "❌ Не удалось изменить настройку напоминания за 15 минут.", err)
			return
		}
		if enabled {
			sendText(bot, chatID, "✅ Напоминание за 15 минут включено.")
		} else {
			sendText(bot, chatID, "✅ Напоминание за 15 минут выключено.")
		}
		sendKeyboard(bot, store, chatID, userID)
		return
	case getReminder5ButtonLabel(store, userID):
		enabled, err := toggleReminderButton(store, userID, "5m")
		if err != nil {
			sendErrorText(bot, chatID, "❌ Не удалось изменить настройку напоминания за 5 минут.", err)
			return
		}
		if enabled {
			sendText(bot, chatID, "✅ Напоминание за 5 минут включено.")
		} else {
			sendText(bot, chatID, "✅ Напоминание за 5 минут выключено.")
		}
		sendKeyboard(bot, store, chatID, userID)
		return
	case getReminderStartButtonLabel(store, userID):
		enabled, err := toggleReminderButton(store, userID, "start")
		if err != nil {
			sendErrorText(bot, chatID, "❌ Не удалось изменить настройку напоминания по началу события.", err)
			return
		}
		if enabled {
			sendText(bot, chatID, "✅ Напоминание по началу события включено.")
		} else {
			sendText(bot, chatID, "✅ Напоминание по началу события выключено.")
		}
		sendKeyboard(bot, store, chatID, userID)
		return
	case btnToday, btnTomorrow, btnWeek, btnNearest:
		log.Printf("button pressed: id=%d username=%q button=%q", userID, username, text)
		u, ok := store.Get(userID)
		if !ok || !userReady(u) {
			sendText(bot, chatID, "🔒 Сначала укажите email и хотя бы одну ICal ссылку.")
			return
		}
		events, err := getOrRefreshUserEvents(eventsCache, client, u, loc)
		if err != nil {
			sendErrorText(bot, chatID, "❌ Не удалось получить календарь. Проверьте ссылку и попробуйте снова.", err)
			return
		}
		now := time.Now().In(loc)
		var response string
		switch text {
		case btnToday:
			response = formatEvents("События на сегодня", filterByDay(events, now, loc), loc)
		case btnTomorrow:
			response = formatEvents("События на завтра", filterByDay(events, now.AddDate(0, 0, 1), loc), loc)
		case btnWeek:
			response = formatEvents("События на неделю", filterForNextDays(events, now, 7), loc)
		case btnNearest:
			e, ok := nearestEvent(events, now)
			if !ok {
				response = "🔎 Ближайших событий не найдено."
			} else {
				response = fmt.Sprintf("🔜 Ближайшее событие:\n%s", formatEventLine(e, loc))
			}
		}
		sendText(bot, chatID, response)
		return
	}

	if mode := inputStates.Get(userID); mode != modeNone {
		switch mode {
		case modeEmail:
			if !isLikelyEmail(text) {
				sendText(bot, chatID, "⚠️ Похоже, это не email. Пример: user@example.com")
				return
			}
			if err := store.SetEmail(userID, text); err != nil {
				sendErrorText(bot, chatID, "❌ Не удалось сохранить email.", err)
				return
			}
			if u, ok := store.Get(userID); ok {
				go func(usr UserRecord) {
					if _, err := refreshUserEventsCache(eventsCache, client, usr, loc); err != nil {
						log.Printf("cache warmup failed after email update: user=%d err=%v", usr.TelegramUserID, err)
					}
				}(u)
			}
			inputStates.Set(userID, modeURLs)
			sendText(bot, chatID, "✅ Email сохранен.")
			sendText(bot, chatID, "🔗 Шаг 2/2. Теперь отправьте одну или несколько ICal ссылок.")
			sendText(bot, chatID, "Пример:\nhttps://calendar.yandex.ru/export/ical/one.ics\nhttps://calendar.yandex.ru/export/ical/two.ics\n\nМожно также через пробел в одной строке.")
			if err := sendPhotoErr(bot, chatID, helpImagePath, "📘 Инструкция и пример формата ссылок."); err != nil {
				log.Printf("help image send failed: user=%d username=%q err=%v", userID, username, err)
			}
		case modeURLs:
			urls, err := parseICalURLsInput(text)
			if err != nil {
				sendErrorText(bot, chatID, "❌ Не удалось разобрать ссылки.", err)
				return
			}
			handleSetURLs(bot, store, eventsCache, client, loc, chatID, userID, username, urls)
			inputStates.Clear(userID)
		}
		return
	}

	if urls, err := parseICalURLsInput(text); err == nil && len(urls) > 0 {
		handleSetURLs(bot, store, eventsCache, client, loc, chatID, userID, username, urls)
		return
	}

	if isLikelyEmail(text) {
		if err := store.SetEmail(userID, text); err != nil {
			sendErrorText(bot, chatID, "❌ Не удалось сохранить email.", err)
			return
		}
		if u, ok := store.Get(userID); ok {
			go func(usr UserRecord) {
				if _, err := refreshUserEventsCache(eventsCache, client, usr, loc); err != nil {
					log.Printf("cache warmup failed after email update: user=%d err=%v", usr.TelegramUserID, err)
				}
			}(u)
		}
		sendText(bot, chatID, "✅ Email сохранен.")
		sendKeyboard(bot, store, chatID, userID)
		return
	}

	sendText(bot, chatID, "🤔 Не понял сообщение. Используйте кнопки или отправьте /start для настройки.")
	if _, ok := store.Get(userID); ok {
		sendKeyboard(bot, store, chatID, userID)
	}
}

func handleSetURLs(bot *tgbotapi.BotAPI, store *UsersStore, eventsCache *UserEventsCache, client *http.Client, loc *time.Location, chatID int64, userID int64, username string, urls []string) {
	log.Printf("set url attempt: id=%d username=%q", userID, username)
	if len(urls) == 0 {
		log.Printf("set url rejected: id=%d username=%q reason=invalid_url", userID, username)
		sendText(bot, chatID, "⚠️ Некорректная ссылка. Нужна webcal/http/https ссылка на ICal.")
		return
	}
	if err := store.SetCalendarURLs(userID, username, urls); err != nil {
		log.Printf("set url failed: id=%d username=%q err=%v", userID, username, err)
		sendErrorText(bot, chatID, "❌ Не удалось сохранить ссылки. Попробуйте позже.", err)
		return
	}
	log.Printf("set url success: id=%d username=%q urls=%d", userID, username, len(urls))
	if u, ok := store.Get(userID); ok {
		go func(usr UserRecord) {
			if _, err := refreshUserEventsCache(eventsCache, client, usr, loc); err != nil {
				log.Printf("cache warmup failed after urls update: user=%d err=%v", usr.TelegramUserID, err)
			}
		}(u)
	}
	sendText(bot, chatID, fmt.Sprintf("✅ Ссылки сохранены (%d). Теперь можно использовать кнопки событий.", len(urls)))
	sendKeyboard(bot, store, chatID, userID)
}

func sendKeyboard(bot *tgbotapi.BotAPI, store *UsersStore, chatID int64, userID int64) {
	weekendLabel := getWeekendButtonLabel(store, userID)
	r15Label := getReminder15ButtonLabel(store, userID)
	r5Label := getReminder5ButtonLabel(store, userID)
	rsLabel := getReminderStartButtonLabel(store, userID)
	kb := tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnToday),
			tgbotapi.NewKeyboardButton(btnTomorrow),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnWeek),
			tgbotapi.NewKeyboardButton(btnNearest),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(r15Label),
			tgbotapi.NewKeyboardButton(r5Label),
			tgbotapi.NewKeyboardButton(rsLabel),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(weekendLabel),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton(btnEditEmail),
			tgbotapi.NewKeyboardButton(btnEditURLs),
		),
	)
	kb.ResizeKeyboard = true
	msg := tgbotapi.NewMessage(chatID, "👇 Выберите действие:")
	msg.ReplyMarkup = kb
	_, _ = bot.Send(msg)
}

func getWeekendButtonLabel(store *UsersStore, userID int64) string {
	u, ok := store.Get(userID)
	if ok && u.WeekendEnabled {
		return btnWeekendOn
	}
	return btnWeekendOff
}

func getReminder15ButtonLabel(store *UsersStore, userID int64) string {
	u, ok := store.Get(userID)
	if !ok || !u.ReminderSettingsInitialized {
		return btnR15Prefix + " ВКЛ"
	}
	if u.Reminder15Enabled {
		return btnR15Prefix + " ВКЛ"
	}
	return btnR15Prefix + " ВЫКЛ"
}

func getReminder5ButtonLabel(store *UsersStore, userID int64) string {
	u, ok := store.Get(userID)
	if !ok || !u.ReminderSettingsInitialized {
		return btnR5Prefix + " ВКЛ"
	}
	if u.Reminder5Enabled {
		return btnR5Prefix + " ВКЛ"
	}
	return btnR5Prefix + " ВЫКЛ"
}

func getReminderStartButtonLabel(store *UsersStore, userID int64) string {
	u, ok := store.Get(userID)
	if !ok || !u.ReminderSettingsInitialized {
		return btnRSPrefix + " ВКЛ"
	}
	if u.ReminderStartEnabled {
		return btnRSPrefix + " ВКЛ"
	}
	return btnRSPrefix + " ВЫКЛ"
}

func toggleReminderButton(store *UsersStore, userID int64, kind string) (bool, error) {
	u, ok := store.Get(userID)
	if !ok || !u.ReminderSettingsInitialized {
		u.Reminder15Enabled = true
		u.Reminder5Enabled = true
		u.ReminderStartEnabled = true
	}
	var next bool
	switch kind {
	case "15m":
		next = !u.Reminder15Enabled
	case "5m":
		next = !u.Reminder5Enabled
	case "start":
		next = !u.ReminderStartEnabled
	default:
		return false, fmt.Errorf("unknown reminder kind: %s", kind)
	}
	return next, store.SetReminderEnabled(userID, kind, next)
}

func userReady(u UserRecord) bool {
	return normalizeEmail(u.Email) != "" && len(sanitizeURLs(u.ICalURLs)) > 0
}

func sanitizeURLs(urls []string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(urls))
	for _, u := range urls {
		nu := normalizeICalURL(strings.TrimSpace(u))
		if !isLikelyURL(nu) {
			continue
		}
		if _, ok := seen[nu]; ok {
			continue
		}
		seen[nu] = struct{}{}
		out = append(out, nu)
	}
	return out
}

func parseICalURLsInput(raw string) ([]string, error) {
	parts := strings.Fields(strings.ReplaceAll(raw, ",", " "))
	urls := sanitizeURLs(parts)
	if len(urls) == 0 {
		return nil, errors.New("не найдено ни одной корректной ICal ссылки")
	}
	return urls, nil
}

func normalizeEmail(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func isLikelyEmail(v string) bool {
	v = normalizeEmail(v)
	if v == "" {
		return false
	}
	re := regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$`)
	return re.MatchString(v)
}

func (s *InputStateStore) Set(userID int64, mode InputMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[userID] = mode
}

func (s *InputStateStore) Get(userID int64) InputMode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state[userID]
}

func (s *InputStateStore) Clear(userID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state, userID)
}

func NewUserEventsCache(ttl time.Duration) *UserEventsCache {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &UserEventsCache{
		ttl:  ttl,
		data: make(map[int64]UserEventsCacheEntry),
	}
}

func (c *UserEventsCache) GetFresh(userID int64) ([]Event, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.data[userID]
	if !ok {
		return nil, false
	}
	if time.Since(e.RefreshedAt) > c.ttl {
		return nil, false
	}
	return e.Events, true
}

func (c *UserEventsCache) Set(userID int64, events []Event, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[userID] = UserEventsCacheEntry{
		Events:      events,
		RefreshedAt: time.Now(),
		Err:         err,
	}
}

func getOrRefreshUserEvents(cache *UserEventsCache, client *http.Client, u UserRecord, loc *time.Location) ([]Event, error) {
	if events, ok := cache.GetFresh(u.TelegramUserID); ok {
		return events, nil
	}
	return refreshUserEventsCache(cache, client, u, loc)
}

func refreshUserEventsCache(cache *UserEventsCache, client *http.Client, u UserRecord, loc *time.Location) ([]Event, error) {
	events, err := fetchEventsForUser(client, u, loc)
	cache.Set(u.TelegramUserID, events, err)
	return events, err
}

func sendText(bot *tgbotapi.BotAPI, chatID int64, text string) {
	if err := sendTextErr(bot, chatID, text); err != nil {
		log.Printf("send text failed: chat=%d err=%v", chatID, err)
	}
}

func sendTextErr(bot *tgbotapi.BotAPI, chatID int64, text string) error {
	// Telegram API rejects invalid UTF-8.
	text = strings.ToValidUTF8(text, " ")
	for _, part := range splitMessageText(text, 3500) {
		part = strings.ToValidUTF8(part, " ")
		msg := tgbotapi.NewMessage(chatID, part)
		if _, err := bot.Send(msg); err != nil {
			return err
		}
	}
	return nil
}

func splitMessageText(text string, maxRunes int) []string {
	if maxRunes <= 0 {
		maxRunes = 3500
	}
	r := []rune(text)
	if len(r) <= maxRunes {
		return []string{text}
	}

	// Keep event blocks intact when text contains event separators.
	eventSeparator := "\n━━━━━━━━━━━━━━\n\n"
	if strings.Contains(text, eventSeparator) {
		return splitBySeparator(text, eventSeparator, maxRunes)
	}

	parts := make([]string, 0, (len(r)/maxRunes)+1)
	for len(r) > maxRunes {
		split := maxRunes
		for i := maxRunes; i > maxRunes/2; i-- {
			if r[i] == '\n' {
				split = i + 1
				break
			}
		}
		parts = append(parts, strings.TrimSpace(string(r[:split])))
		r = r[split:]
	}
	if len(r) > 0 {
		parts = append(parts, strings.TrimSpace(string(r)))
	}

	// guard against empty chunks after TrimSpace
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{text}
	}
	return out
}

func splitBySeparator(text, sep string, maxRunes int) []string {
	blocks := strings.Split(text, sep)
	result := make([]string, 0, len(blocks))
	current := ""

	for _, raw := range blocks {
		block := strings.TrimSpace(raw)
		if block == "" {
			continue
		}

		candidate := block
		if current != "" {
			candidate = current + sep + block
		}

		if len([]rune(candidate)) <= maxRunes {
			current = candidate
			continue
		}

		if current != "" {
			result = append(result, strings.TrimSpace(current))
		}

		// If one block is too large, fallback to generic split for this block.
		if len([]rune(block)) > maxRunes {
			result = append(result, splitMessageText(block, maxRunes)...)
			current = ""
			continue
		}
		current = block
	}

	if strings.TrimSpace(current) != "" {
		result = append(result, strings.TrimSpace(current))
	}

	return result
}

func sendPhotoErr(bot *tgbotapi.BotAPI, chatID int64, imagePath, caption string) error {
	imagePath = strings.TrimSpace(imagePath)
	if imagePath == "" {
		return errors.New("help image path is empty")
	}
	photo := tgbotapi.NewPhoto(chatID, tgbotapi.FilePath(imagePath))
	photo.Caption = caption
	_, err := bot.Send(photo)
	return err
}

func sendErrorText(bot *tgbotapi.BotAPI, chatID int64, message string, err error) {
	if err == nil {
		sendText(bot, chatID, message)
		return
	}
	sendText(bot, chatID, fmt.Sprintf("%s\nОшибка: %s", message, truncateErr(err.Error(), 350)))
}

func truncateErr(v string, limit int) string {
	v = strings.TrimSpace(v)
	r := []rune(v)
	if len(r) <= limit {
		return v
	}
	if limit < 4 {
		return string(r[:limit])
	}
	return string(r[:limit-3]) + "..."
}

func isLikelyURL(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") || strings.HasPrefix(s, "webcal://")
}

func normalizeICalURL(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(strings.ToLower(s), "webcal://") {
		return "https://" + s[len("webcal://"):]
	}
	return s
}

func fetchEvents(client *http.Client, icalURL string, loc *time.Location) ([]Event, error) {
	req, err := http.NewRequest(http.MethodGet, icalURL, nil)
	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status: %s", res.Status)
	}

	cal, err := ical.ParseCalendar(res.Body)
	if err != nil {
		return nil, err
	}

	now := time.Now().In(loc)
	windowFrom := now.Add(-24 * time.Hour)
	windowTo := now.AddDate(0, 0, 90)

	var out []Event
	seen := make(map[string]struct{})
	for _, e := range cal.Events() {
		dtStart, ok := parseEventDateTime(e.GetProperty(ical.ComponentPropertyDtStart), loc)
		if !ok {
			continue
		}
		dtEnd, ok := parseEventDateTime(e.GetProperty(ical.ComponentPropertyDtEnd), loc)
		if !ok {
			dtEnd = dtStart.Add(1 * time.Hour)
		}

		summary := "Без названия"
		if p := e.GetProperty(ical.ComponentPropertySummary); p != nil {
			if v := strings.TrimSpace(p.Value); v != "" {
				summary = v
			}
		}
		description := ""
		if p := e.GetProperty(ical.ComponentPropertyDescription); p != nil {
			description = strings.TrimSpace(p.Value)
		}
		locationText := ""
		if p := e.GetProperty(ical.ComponentPropertyLocation); p != nil {
			locationText = strings.TrimSpace(p.Value)
		}
		uid := fmt.Sprintf("%s|%d", summary, dtStart.Unix())
		if p := e.GetProperty(ical.ComponentPropertyUniqueId); p != nil && p.Value != "" {
			uid = p.Value
		}
		duration := dtEnd.Sub(dtStart)
		if duration <= 0 {
			duration = time.Hour
		}

		eventURL := ""
		if p := e.GetProperty(ical.ComponentPropertyUrl); p != nil {
			eventURL = strings.TrimSpace(p.Value)
		}
		if eventURL == "" {
			if p := e.GetProperty(ical.ComponentPropertyDescription); p != nil {
				eventURL = extractFirstURL(p.Value)
			}
		}

		organizerProp := e.GetProperty(ical.ComponentPropertyOrganizer)
		organizer := parsePersonProperty(organizerProp)
		organizerEmail := extractEmailFromProperty(organizerProp)
		participants, participantEmails := parseParticipantList(e.GetProperties(ical.ComponentPropertyAttendee))

		base := Event{
			UID:               uid,
			Title:             summary,
			Start:             dtStart,
			End:               dtEnd,
			Description:       description,
			Location:          locationText,
			URL:               eventURL,
			Organizer:         organizer,
			OrganizerEmail:    organizerEmail,
			Participants:      participants,
			ParticipantEmails: participantEmails,
		}

		rruleProp := e.GetProperty(ical.ComponentPropertyRrule)
		if rruleProp != nil && strings.TrimSpace(rruleProp.Value) != "" {
			exDates := toUnixSet(parseDateListProperties(e.GetProperties(ical.ComponentPropertyExdate), loc))
			rDates := parseDateListProperties(e.GetProperties(ical.ComponentPropertyRdate), loc)

			for _, occStart := range expandRecurrence(rruleProp.Value, dtStart, windowFrom, windowTo) {
				if isExcludedOccurrence(occStart, exDates) {
					continue
				}
				occ := base
				occ.Start = occStart
				occ.End = occStart.Add(duration)
				addEventUnique(&out, seen, occ)
			}
			for _, occStart := range rDates {
				if occStart.Before(windowFrom) || !occStart.Before(windowTo) {
					continue
				}
				if isExcludedOccurrence(occStart, exDates) {
					continue
				}
				occ := base
				occ.Start = occStart
				occ.End = occStart.Add(duration)
				addEventUnique(&out, seen, occ)
			}
			continue
		}

		if dtStart.Before(windowFrom) || !dtStart.Before(windowTo) {
			continue
		}
		addEventUnique(&out, seen, base)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out, nil
}

func fetchEventsForUser(client *http.Client, u UserRecord, loc *time.Location) ([]Event, error) {
	urls := sanitizeURLs(u.ICalURLs)
	if len(urls) == 0 {
		return nil, errors.New("нет ICal ссылок")
	}

	all := make([]Event, 0)
	seen := make(map[string]struct{})
	var errs []string
	for _, url := range urls {
		events, err := fetchEvents(client, url, loc)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", url, err))
			continue
		}
		for _, e := range events {
			key := fmt.Sprintf("%s|%d", e.UID, e.Start.Unix())
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			all = append(all, e)
		}
	}
	if len(all) == 0 && len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}

	filtered := filterEventsByUserEmail(all, u.Email)
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Start.Before(filtered[j].Start) })
	return filtered, nil
}

func filterEventsByUserEmail(events []Event, email string) []Event {
	email = normalizeEmail(email)
	if email == "" {
		return nil
	}
	out := make([]Event, 0, len(events))
	for _, e := range events {
		if eventHasEmail(e, email) {
			out = append(out, e)
		}
	}
	return out
}

func eventHasEmail(e Event, userEmail string) bool {
	if userEmail == "" {
		return false
	}
	if normalizeEmail(e.OrganizerEmail) == userEmail {
		return true
	}
	for _, p := range e.ParticipantEmails {
		if normalizeEmail(p) == userEmail {
			return true
		}
	}
	return false
}

func extractEmailFromText(v string) string {
	re := regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	return strings.TrimSpace(re.FindString(v))
}

func extractEmailFromProperty(p *ical.IANAProperty) string {
	if p == nil {
		return ""
	}
	// Most iCal exports store email in value like: mailto:user@example.com
	if e := extractEmailFromText(strings.TrimSpace(p.Value)); e != "" {
		return normalizeEmail(e)
	}
	// Fallback: sometimes email can appear in params.
	for _, vals := range p.ICalParameters {
		for _, v := range vals {
			if e := extractEmailFromText(v); e != "" {
				return normalizeEmail(e)
			}
		}
	}
	return ""
}

func addEventUnique(dst *[]Event, seen map[string]struct{}, e Event) {
	key := fmt.Sprintf("%s|%d", e.UID, e.Start.Unix())
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	*dst = append(*dst, e)
}

func expandRecurrence(ruleText string, dtStart, from, to time.Time) []time.Time {
	opts, err := rrule.StrToROption(strings.TrimSpace(ruleText))
	if err != nil {
		return []time.Time{dtStart}
	}
	opts.Dtstart = dtStart

	r, err := rrule.NewRRule(*opts)
	if err != nil {
		return []time.Time{dtStart}
	}
	return r.Between(from, to, true)
}

func parseDateListProperties(props []*ical.IANAProperty, defaultLoc *time.Location) []time.Time {
	out := make([]time.Time, 0)
	for _, p := range props {
		if p == nil {
			continue
		}
		parts := strings.Split(p.Value, ",")
		for _, part := range parts {
			tmp := &ical.IANAProperty{BaseProperty: ical.BaseProperty{
				Value:          strings.TrimSpace(part),
				ICalParameters: p.ICalParameters,
			}}
			t, ok := parseEventDateTime(tmp, defaultLoc)
			if !ok {
				continue
			}
			out = append(out, t)
		}
	}
	return out
}

func toUnixSet(times []time.Time) map[int64]struct{} {
	out := make(map[int64]struct{}, len(times))
	for _, t := range times {
		out[t.Unix()] = struct{}{}
	}
	return out
}

func isExcludedOccurrence(start time.Time, excludes map[int64]struct{}) bool {
	_, ok := excludes[start.Unix()]
	return ok
}

func parsePersonProperty(p *ical.IANAProperty) string {
	if p == nil {
		return ""
	}
	name := ""
	if p.ICalParameters != nil {
		if cn, ok := p.ICalParameters["CN"]; ok && len(cn) > 0 {
			name = strings.TrimSpace(cn[0])
		}
	}
	raw := strings.TrimSpace(p.Value)
	raw = strings.TrimPrefix(strings.TrimPrefix(raw, "MAILTO:"), "mailto:")
	if name != "" && raw != "" {
		return fmt.Sprintf("%s <%s>", name, raw)
	}
	if name != "" {
		return name
	}
	return raw
}

func parseParticipantList(attendees []*ical.IANAProperty) ([]string, []string) {
	out := make([]string, 0, len(attendees))
	emails := make([]string, 0, len(attendees))
	for _, a := range attendees {
		person := parsePersonProperty(a)
		if person == "" {
			continue
		}
		out = append(out, person)
		if email := extractEmailFromProperty(a); email != "" {
			emails = append(emails, email)
		}
	}
	return out, emails
}

func extractFirstURL(s string) string {
	re := regexp.MustCompile(`https?://[^\s]+`)
	return strings.TrimSpace(re.FindString(s))
}

func parseEventDateTime(prop *ical.IANAProperty, defaultLoc *time.Location) (time.Time, bool) {
	if prop == nil {
		return time.Time{}, false
	}
	raw := strings.TrimSpace(prop.Value)
	if raw == "" {
		return time.Time{}, false
	}

	loc := defaultLoc
	if prop.ICalParameters != nil {
		if tzVals, ok := prop.ICalParameters["TZID"]; ok && len(tzVals) > 0 {
			if l, err := time.LoadLocation(tzVals[0]); err == nil {
				loc = l
			}
		}
	}

	layouts := []string{"20060102T150405Z", "20060102T150405", "20060102"}
	for _, layout := range layouts {
		switch layout {
		case "20060102T150405Z":
			if t, err := time.Parse(layout, raw); err == nil {
				return t.In(defaultLoc), true
			}
		case "20060102T150405", "20060102":
			if t, err := time.ParseInLocation(layout, raw, loc); err == nil {
				return t.In(defaultLoc), true
			}
		}
	}
	return time.Time{}, false
}

func filterByDay(events []Event, day time.Time, loc *time.Location) []Event {
	start := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, loc)
	end := start.Add(24 * time.Hour)
	return filterByRange(events, start, end)
}

func filterForNextDays(events []Event, from time.Time, days int) []Event {
	to := from.AddDate(0, 0, days)
	return filterByRange(events, from, to)
}

func filterByRange(events []Event, from, to time.Time) []Event {
	out := make([]Event, 0)
	for _, e := range events {
		if e.Start.Before(from) || !e.Start.Before(to) {
			continue
		}
		out = append(out, e)
	}
	return out
}

func nearestEvent(events []Event, now time.Time) (Event, bool) {
	for _, e := range events {
		if e.Start.After(now) || e.Start.Equal(now) {
			return e, true
		}
	}
	return Event{}, false
}

func formatEvents(header string, events []Event, loc *time.Location) string {
	if len(events) == 0 {
		return header + ":\n🫥 Нет событий."
	}
	var b strings.Builder
	b.WriteString(header)
	b.WriteString(":\n")
	for i, e := range events {
		b.WriteString(fmt.Sprintf("%d. %s\n", i+1, formatEventLine(e, loc)))
		if i < len(events)-1 {
			b.WriteString("\n━━━━━━━━━━━━━━\n\n")
		}
	}
	return strings.TrimSpace(b.String())
}

func formatEventLine(e Event, loc *time.Location) string {
	lines := []string{
		"📌 " + e.Title,
		fmt.Sprintf("🕒 Время: %s - %s", e.Start.In(loc).Format("02.01 15:04"), e.End.In(loc).Format("15:04")),
	}
	if e.Location != "" {
		lines = append(lines, "📍 Место: "+e.Location)
	}
	if e.Description != "" {
		lines = append(lines, "📝 Описание: "+truncateErr(e.Description, 500))
	}
	if e.Organizer != "" {
		lines = append(lines, "👤 Организатор: "+e.Organizer)
	}
	if len(e.Participants) > 0 {
		lines = append(lines, "👥 Участники: "+strings.Join(e.Participants, ", "))
	}
	if e.URL != "" {
		lines = append(lines, "🔗 Ссылка: "+e.URL)
	}
	return strings.Join(lines, "\n")
}

func runReminderLoop(ctx context.Context, bot *tgbotapi.BotAPI, store *UsersStore, eventsCache *UserEventsCache, client *http.Client, interval time.Duration, loc *time.Location, engine *ReminderEngine) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	engine.checkAndNotify(bot, store, eventsCache, client, loc, interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			engine.checkAndNotify(bot, store, eventsCache, client, loc, interval)
		}
	}
}

func (r *ReminderEngine) checkAndNotify(bot *tgbotapi.BotAPI, store *UsersStore, eventsCache *UserEventsCache, client *http.Client, loc *time.Location, interval time.Duration) {
	now := time.Now().In(loc)
	users := store.List()
	for _, u := range users {
		if !userReady(u) {
			continue
		}
		events, err := getOrRefreshUserEvents(eventsCache, client, u, loc)
		if err != nil {
			log.Printf("reminder: user %d fetch failed: %v", u.TelegramUserID, err)
			continue
		}
		for _, e := range events {
			if !u.WeekendEnabled && isWeekend(e.Start.In(loc)) {
				continue
			}
			if u.Reminder15Enabled && shouldTriggerBefore(now, e.Start, 15*time.Minute, interval) {
				r.sendOnce(bot, u.TelegramUserID, e, "15m", "❗️ через 15 минут начнется встреча ❗️", loc)
			}
			if u.Reminder5Enabled && shouldTriggerBefore(now, e.Start, 5*time.Minute, interval) {
				r.sendOnce(bot, u.TelegramUserID, e, "5m", "❗️ через 5 минут начнется встреча ❗️", loc)
			}
			if u.ReminderStartEnabled && shouldTriggerStartedRecently(now, e.Start, 10*time.Minute) {
				r.sendOnce(bot, u.TelegramUserID, e, "start", "❗️ Встреча началась ❗️", loc)
			}
		}
	}
	r.cleanup(now)
}

func (r *ReminderEngine) sendOnce(bot *tgbotapi.BotAPI, userID int64, e Event, phase, title string, loc *time.Location) {
	key := fmt.Sprintf("%d|%s|%s|%d", userID, e.UID, phase, e.Start.Unix())

	r.mu.Lock()
	if _, exists := r.sent[key]; exists {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	msg := fmt.Sprintf("%s\n%s", title, formatEventLine(e, loc))
	if err := sendTextErr(bot, userID, msg); err != nil {
		log.Printf("reminder send failed: user=%d phase=%s event=%s err=%v", userID, phase, e.UID, err)
		return
	}

	r.mu.Lock()
	r.sent[key] = time.Now()
	r.mu.Unlock()
	log.Printf("reminder sent: user=%d phase=%s event=%s start=%s", userID, phase, e.UID, e.Start.In(loc).Format(time.RFC3339))
}

func (r *ReminderEngine) cleanup(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	threshold := now.Add(-48 * time.Hour)
	for k, v := range r.sent {
		if v.Before(threshold) {
			delete(r.sent, k)
		}
	}
}

func runDailyDigestLoop(ctx context.Context, bot *tgbotapi.BotAPI, store *UsersStore, eventsCache *UserEventsCache, client *http.Client, digestHHMM string, loc *time.Location) {
	hour, minute, _ := parseClockHHMM(digestHHMM)

	for {
		now := time.Now().In(loc)
		next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, loc)
		if !next.After(now) {
			next = next.Add(24 * time.Hour)
		}

		timer := time.NewTimer(next.Sub(now))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			sendDailyDigest(bot, store, eventsCache, client, loc)
		}
	}
}

func sendDailyDigest(bot *tgbotapi.BotAPI, store *UsersStore, eventsCache *UserEventsCache, client *http.Client, loc *time.Location) {
	today := time.Now().In(loc)
	for _, u := range store.List() {
		if !userReady(u) {
			continue
		}
		if !u.WeekendEnabled && isWeekend(today) {
			continue
		}
		events, err := getOrRefreshUserEvents(eventsCache, client, u, loc)
		if err != nil {
			log.Printf("daily digest: user %d fetch failed: %v", u.TelegramUserID, err)
			continue
		}
		todayEvents := filterByDay(events, today, loc)
		text := formatEvents("События на сегодня", todayEvents, loc)
		sendText(bot, u.TelegramUserID, text)
	}
}

func isWeekend(t time.Time) bool {
	wd := t.Weekday()
	return wd == time.Saturday || wd == time.Sunday
}

func NewDailyLogWriter(logDir, rotateHHMM string, retentionDays int, loc *time.Location) (*DailyLogWriter, error) {
	logDir = strings.TrimSpace(logDir)
	if logDir == "" {
		return nil, errors.New("log dir is empty")
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	h, m, err := parseClockHHMM(rotateHHMM)
	if err != nil {
		return nil, err
	}
	w := &DailyLogWriter{
		logDir:     logDir,
		loc:        loc,
		retention:  retentionDays,
		hour:       h,
		minute:     m,
		stopCh:     make(chan struct{}),
		currentDay: time.Now().In(loc).Format("2006-01-02"),
	}
	if err := w.openCurrentLocked(); err != nil {
		return nil, err
	}
	if err := w.cleanupOldArchives(); err != nil {
		log.Printf("log cleanup warning: %v", err)
	}
	go w.rotateLoop()
	return w, nil
}

func (w *DailyLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		if err := w.openCurrentLocked(); err != nil {
			return 0, err
		}
	}
	return w.file.Write(p)
}

func (w *DailyLogWriter) Close() error {
	close(w.stopCh)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *DailyLogWriter) rotateLoop() {
	for {
		next := w.nextRotationTime(time.Now().In(w.loc))
		timer := time.NewTimer(time.Until(next))
		select {
		case <-w.stopCh:
			timer.Stop()
			return
		case <-timer.C:
			if err := w.rotate(); err != nil {
				log.Printf("log rotate error: %v", err)
			}
		}
	}
}

func (w *DailyLogWriter) nextRotationTime(now time.Time) time.Time {
	next := time.Date(now.Year(), now.Month(), now.Day(), w.hour, w.minute, 0, 0, w.loc)
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

func (w *DailyLogWriter) rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		if err := w.openCurrentLocked(); err != nil {
			return err
		}
	}

	archivePlain := filepath.Join(w.logDir, fmt.Sprintf("bot-%s.log", w.currentDay))
	currentPath := filepath.Join(w.logDir, "current.log")

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close current log: %w", err)
	}
	w.file = nil
	if err := os.Rename(currentPath, archivePlain); err != nil {
		return fmt.Errorf("archive current log: %w", err)
	}
	if err := gzipFile(archivePlain); err != nil {
		return fmt.Errorf("gzip archive: %w", err)
	}

	w.currentDay = time.Now().In(w.loc).Format("2006-01-02")
	if err := w.openCurrentLocked(); err != nil {
		return err
	}
	return w.cleanupOldArchives()
}

func (w *DailyLogWriter) openCurrentLocked() error {
	currentPath := filepath.Join(w.logDir, "current.log")
	f, err := os.OpenFile(currentPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open current log: %w", err)
	}
	w.file = f
	return nil
}

func (w *DailyLogWriter) cleanupOldArchives() error {
	entries, err := os.ReadDir(w.logDir)
	if err != nil {
		return err
	}
	cutoff := time.Now().In(w.loc).AddDate(0, 0, -w.retention)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "bot-") || !strings.HasSuffix(name, ".log.gz") {
			continue
		}
		datePart := strings.TrimSuffix(strings.TrimPrefix(name, "bot-"), ".log.gz")
		d, err := time.ParseInLocation("2006-01-02", datePart, w.loc)
		if err != nil {
			continue
		}
		if d.Before(cutoff) {
			_ = os.Remove(filepath.Join(w.logDir, name))
		}
	}
	return nil
}

func gzipFile(srcPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dstPath := srcPath + ".gz"
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	gz := gzip.NewWriter(dst)
	if _, err := io.Copy(gz, src); err != nil {
		gz.Close()
		dst.Close()
		return err
	}
	if err := gz.Close(); err != nil {
		dst.Close()
		return err
	}
	if err := dst.Close(); err != nil {
		return err
	}
	return os.Remove(srcPath)
}

func shouldTriggerBefore(now, start time.Time, before, interval time.Duration) bool {
	if interval <= 0 {
		interval = 5 * time.Minute
	}

	tolerance := interval
	if tolerance < 30*time.Second {
		tolerance = 30 * time.Second
	}
	left := start.Sub(now)
	return left <= before && left > before-tolerance
}

func shouldTriggerStartedRecently(now, start time.Time, maxDelay time.Duration) bool {
	if maxDelay <= 0 {
		return false
	}
	since := now.Sub(start)
	return since >= 0 && since <= maxDelay
}
