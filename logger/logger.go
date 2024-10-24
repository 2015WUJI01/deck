package logger

import (
	"context"
	"github.com/2015wuji01/deck/utils"
	"io"
	"log"
	"os"
)

// Colors
const (
	Reset       = "\033[0m"
	Red         = "\033[31m"
	Green       = "\033[32m"
	Yellow      = "\033[33m"
	Blue        = "\033[34m"
	Magenta     = "\033[35m"
	Cyan        = "\033[36m"
	White       = "\033[37m"
	BlueBold    = "\033[34;1m"
	MagentaBold = "\033[35;1m"
	RedBold     = "\033[31;1m"
	YellowBold  = "\033[33;1m"
)

// LogLevel log level
type LogLevel int

const (
	// Silent silent log level
	Silent LogLevel = iota + 1
	// Error error log level
	Error
	// Warn warn log level
	Warn
	// Info info log level
	Info
	// 底层日志，用于调试
	Trace
)

// Writer log writer interface
type Writer interface {
	Printf(string, ...interface{})
}

// Config logger config
type Config struct {
	Colorful bool
	LogLevel LogLevel
}

// Interface logger interface
type Interface interface {
	LogMode(LogLevel) Interface
	Trace(context.Context, string, ...interface{})
	Info(context.Context, string, ...interface{})
	Warn(context.Context, string, ...interface{})
	Error(context.Context, string, ...interface{})
}

var (
	// Discard logger will print any log to io.Discard
	Discard = New(log.New(io.Discard, "", log.LstdFlags), Config{})
	// Default Default logger
	Default = New(log.New(os.Stdout, "\r\n", log.LstdFlags), Config{
		LogLevel: Warn,
		Colorful: true,
	})
)

// New initialize logger
func New(writer Writer, config Config) Interface {
	var (
		traceStr = "%s\n[trace] "
		infoStr  = "%s\n[info] "
		warnStr  = "%s\n[warn] "
		errStr   = "%s\n[error] "
	)

	if config.Colorful {
		traceStr = Cyan + "%s\n" + Reset + Cyan + "[trace] " + Reset
		infoStr = Green + "%s\n" + Reset + Green + "[info] " + Reset
		warnStr = BlueBold + "%s\n" + Reset + Magenta + "[warn] " + Reset
		errStr = Magenta + "%s\n" + Reset + Red + "[error] " + Reset
	}

	return &logger{
		Writer:   writer,
		Config:   config,
		traceStr: traceStr,
		infoStr:  infoStr,
		warnStr:  warnStr,
		errStr:   errStr,
	}
}

type logger struct {
	Writer
	Config
	traceStr, infoStr, warnStr, errStr string
}

// LogMode log mode
func (l *logger) LogMode(level LogLevel) Interface {
	newlogger := *l
	newlogger.LogLevel = level
	return &newlogger
}

// Trace print info
func (l *logger) Trace(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= Trace {
		l.Printf(l.traceStr+msg, append([]interface{}{utils.FileWithLineNum()}, data...)...)
	}
}

// Info print info
func (l *logger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= Info {
		l.Printf(l.infoStr+msg, append([]interface{}{utils.FileWithLineNum()}, data...)...)
	}
}

// Warn print warn messages
func (l *logger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= Warn {
		l.Printf(l.warnStr+msg, append([]interface{}{utils.FileWithLineNum()}, data...)...)
	}
}

// Error print error messages
func (l *logger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= Error {
		l.Printf(l.errStr+msg, append([]interface{}{utils.FileWithLineNum()}, data...)...)
	}
}
