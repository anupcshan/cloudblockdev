// Command cloudblockdev provides an NBD server backed by S3-compatible storage.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/anupcshan/cloudblockdev/internal/cloudblock"
	"github.com/anupcshan/cloudblockdev/internal/config"
	"github.com/anupcshan/cloudblockdev/internal/nbd"
	"github.com/anupcshan/cloudblockdev/internal/nbdclient"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Set by -ldflags at build time.
var (
	version = "dev"
	date    = "unknown"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		cmdServe(os.Args[2:])
	case "version":
		fmt.Printf("cloudblockdev %s (built %s)\n", version, date)
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `cloudblockdev - Block device server backed by S3-compatible storage

Usage:
  cloudblockdev serve -config <file> [-force-reconnect]

Options:
  -force-reconnect  Disconnect existing device before connecting
                    (required when changing device size)

Example config file (YAML):

%sConnecting:
  nbd-client -N <export_name> <host> 10809 /dev/nbdX
`, config.ExampleConfig)
}

// cmdServe starts the NBD server.
func cmdServe(args []string) {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	var configPath string
	var forceReconnect bool
	var cpuprofile string
	var memprofile string
	fs.StringVar(&configPath, "config", "", "Path to config file (required)")
	fs.BoolVar(&forceReconnect, "force-reconnect", false, "Disconnect existing device before connecting (required when changing size)")
	fs.StringVar(&cpuprofile, "cpuprofile", "", "Write CPU profile to file")
	fs.StringVar(&memprofile, "memprofile", "", "Write memory profile to file on exit")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: cloudblockdev serve -config <file> [-force-reconnect] [-cpuprofile <file>] [-memprofile <file>]\n\nOptions:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if configPath == "" {
		fmt.Fprintf(os.Stderr, "Error: -config is required\n")
		fs.Usage()
		os.Exit(1)
	}

	// Start CPU profiling if requested
	var profileFile *os.File
	if cpuprofile != "" {
		var err error
		profileFile, err = os.Create(cpuprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating CPU profile: %v\n", err)
			os.Exit(1)
		}
		log.Printf("CPU profiling enabled, writing to %s", cpuprofile)
		pprof.StartCPUProfile(profileFile)
	}
	stopProfiling := func() {
		if profileFile != nil {
			pprof.StopCPUProfile()
			profileFile.Close()
			log.Printf("CPU profile written")
		}
		if memprofile != "" {
			f, err := os.Create(memprofile)
			if err != nil {
				log.Printf("Error creating memory profile: %v", err)
				return
			}
			runtime.GC()
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Printf("Error writing memory profile: %v", err)
			}
			f.Close()
			log.Printf("Memory profile written to %s", memprofile)
		}
	}
	defer stopProfiling()

	// Load config
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Parse device size
	deviceSize, err := config.ParseSize(cfg.Device.Size)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid device.size: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// Parse max write buffer size
	maxWriteBuffer, err := config.ParseSize(cfg.Device.MaxWriteBuffer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid device.max_write_buffer: %v\n", err)
		os.Exit(1)
	}

	log.Printf("Device config: size=%d max_write_buffer=%d", deviceSize, maxWriteBuffer)

	// Create blob store
	var store cloudblock.BlobStore
	if cfg.FileStore.Dir != "" {
		// Use filesystem-backed store (for testing/tracing)
		var latency time.Duration
		if cfg.FileStore.Latency != "" {
			latency, err = time.ParseDuration(cfg.FileStore.Latency)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid filestore.latency: %v\n", err)
				os.Exit(1)
			}
		}
		store, err = cloudblock.NewFileBlobStore(cloudblock.FileBlobStoreConfig{
			Dir:     cfg.FileStore.Dir,
			Latency: latency,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating file blob store: %v\n", err)
			os.Exit(1)
		}
		log.Printf("Using file store: dir=%s latency=%v", cfg.FileStore.Dir, latency)
	} else {
		// Use S3 (default)
		store, err = cloudblock.NewS3BlobStore(ctx, cloudblock.S3BlobStoreConfig{
			Bucket:    cfg.S3.Bucket,
			Prefix:    cfg.S3.Prefix,
			Endpoint:  cfg.S3.Endpoint,
			Region:    cfg.S3.Region,
			AccessKey: cfg.S3.AccessKey,
			SecretKey: cfg.S3.SecretKey,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating S3 blob store: %v\n", err)
			os.Exit(1)
		}
	}

	// Parse GC interval
	var gcInterval time.Duration
	if cfg.GC.Interval != "" {
		gcInterval, err = time.ParseDuration(cfg.GC.Interval)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid gc.interval: %v\n", err)
			os.Exit(1)
		}
	}

	// Create cloud block device
	device, err := cloudblock.NewDevice(cloudblock.DeviceConfig{
		Store:                    store,
		Size:                     deviceSize,
		GCEnabled:                *cfg.GC.Enabled,
		GCInterval:               gcInterval,
		MaxOutstandingWriteBytes: maxWriteBuffer,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating device: %v\n", err)
		os.Exit(1)
	}

	// Start the device (background goroutines for concurrent I/O)
	if err := device.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting device: %v\n", err)
		os.Exit(1)
	}

	// Load index from S3
	if err := device.LoadIndex(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error loading index: %v\n", err)
		os.Exit(1)
	}

	// Create NBD server
	nbdServer := nbd.NewServer(nbd.Config{
		Device:     device,
		ExportName: cfg.Device.ExportName,
	})

	var nbdMgr *nbdclient.DeviceManager
	var tcpListener net.Listener

	// Set up kernel NBD device if enabled
	if cfg.NBD.Enabled {
		deadConnTimeout, err := config.ParseDuration(cfg.NBD.DeadConnTimeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid nbd.dead_conn_timeout: %v\n", err)
			os.Exit(1)
		}

		nbdMgr = nbdclient.NewDeviceManager(nbdclient.DeviceConfig{
			Index:           *cfg.NBD.Index,
			Size:            uint64(deviceSize),
			BlockSize:       4096,
			DeadConnTimeout: deadConnTimeout,
			ForceReconnect:  forceReconnect,
		})

		if err := nbdMgr.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "Error setting up NBD device: %v\n", err)
			os.Exit(1)
		}

		log.Printf("Kernel NBD device ready at %s", nbdMgr.DevicePath())

		// Handle NBD protocol on our end of the socketpair
		go func() {
			if err := nbdServer.HandleDirectConnection(nbdMgr.ServerConn()); err != nil {
				log.Printf("NBD connection error: %v", err)
			}
		}()
	}

	// Start external TCP listener if configured
	if cfg.Server.Listen != "" {
		var err error
		tcpListener, err = net.Listen("tcp", cfg.Server.Listen)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listening: %v\n", err)
			os.Exit(1)
		}
		log.Printf("Starting NBD server on %s", cfg.Server.Listen)
		log.Printf("Export: %s (size=%d)", cfg.Device.ExportName, deviceSize)
		log.Printf("Connect with: nbd-client -N %s <host> 10809 /dev/nbdX", cfg.Device.ExportName)
	}

	// Start Prometheus metrics server if configured
	if cfg.Metrics.Listen != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			mux.HandleFunc("/debug/pprof/", httppprof.Index)
			log.Printf("Starting Prometheus metrics server on %s", cfg.Metrics.Listen)
			if err := http.ListenAndServe(cfg.Metrics.Listen, mux); err != nil {
				log.Printf("Metrics server error: %v", err)
			}
		}()
	}

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down...")

		// Flush device to S3 before exit
		device.Close()

		// Close NBD manager (socket closes, kernel waits for reconnect)
		if nbdMgr != nil {
			nbdMgr.Close()
		}

		// Stop accepting new TCP connections
		nbdServer.Shutdown()

		// Stop CPU profiling (must happen before exit)
		stopProfiling()

		os.Exit(0)
	}()

	// Serve connections (blocks)
	if tcpListener != nil {
		if err := nbdServer.Serve(tcpListener); err != nil {
			fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
			os.Exit(1)
		}
	} else {
		// No TCP listener, just wait for signals
		select {}
	}
}
