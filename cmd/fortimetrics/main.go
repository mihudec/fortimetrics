package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mihudec/fortimetrics/internal/config"
	"github.com/mihudec/fortimetrics/internal/pipeline"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	path := flag.String("config", "fortimetrics.yml", "YAML configuration path")
	once := flag.Bool("once", false, "collect once, drain publishers, then exit")
	check := flag.Bool("check-config", false, "validate configuration without network requests")
	showVersion := flag.Bool("version", false, "print build metadata and exit")
	logLevel := flag.String("log-level", "info", "logging verbosity: debug, info, warn, or error")
	flag.Parse()
	if *showVersion {
		fmt.Printf("fortimetrics %s (commit %s, built %s)\n", version, commit, date)
		return
	}
	os.Exit(run(*path, *once, *check, *logLevel))
}

func run(path string, once, check bool, logLevel string) int {
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil || level < zap.DebugLevel || level > zap.ErrorLevel {
		fmt.Fprintln(os.Stderr, "invalid -log-level: expected debug, info, warn, or error")
		return 2
	}
	logger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.Lock(os.Stderr),
		level,
	))
	restore := zap.ReplaceGlobals(logger)
	defer restore()
	log := logger.Sugar()
	f, e := os.Open(path)
	if e != nil {
		log.Errorw("open config", "error", e)
		return 1
	}
	c, e := config.Load(f)
	f.Close()
	if e != nil {
		log.Errorw("invalid config", "error", e)
		return 1
	}
	if check {
		log.Info("configuration valid")
		return 0
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if e = pipeline.Run(ctx, c, once); e != nil {
		log.Errorw("collector stopped", "error", e)
		return 1
	}
	return 0
}
