package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/coreos/torus/block"

	"github.com/coreos/pkg/progressutil"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

var (
	dumpCommand = &cobra.Command{
		Use:   "dump VOLUME OUTPUT_FILE",
		Short: "dump the contents of a volume to an output file",
		Run:   dumpAction,
	}

	loadCommand = &cobra.Command{
		Use:   "load INPUT_FILE VOLUME [SIZE]",
		Short: "load the contents of a file into a new volume",
		Run:   loadAction,
	}

	progress bool
)

func init() {
	dumpCommand.Flags().BoolVarP(&progress, "progress", "p", false, "show progress")
	rootCommand.AddCommand(dumpCommand)
	loadCommand.Flags().BoolVarP(&progress, "progress", "p", false, "show progress")
	rootCommand.AddCommand(loadCommand)
}

func dumpAction(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		os.Exit(1)
	}
	output, err := getWriterFromArg(args[1])
	if err != nil {
		die("couldn't open output: %v", err)
	}
	srv := createServer()
	blockvol, err := block.OpenBlockVolume(srv, args[0])
	if err != nil {
		die("couldn't open block volume %s: %v", args[0], err)
	}
	tempsnap := fmt.Sprintf("temp-dump-%d", os.Getpid())
	err = blockvol.SaveSnapshot(tempsnap)
	if err != nil {
		die("couldn't snapshot: %v", err)
	}
	bf, err := blockvol.OpenSnapshot(tempsnap)
	if err != nil {
		die("couldn't open snapshot: %v", err)
	}

	size := int64(bf.Size())
	if progress {
		pb := new(progressutil.CopyProgressPrinter)
		pb.AddCopy(bf, path.Base(args[0]), size, output)
		err := pb.PrintAndWait(os.Stdout, 500*time.Millisecond, nil)
		if err != nil {
			die("couldn't copy: %v", err)
		}
	} else {
		n, err := io.Copy(output, bf)
		if err != nil {
			die("couldn't copy: %v", err)
		}

		if n != size {
			die("short read of %q", args[0])
		}
	}

	err = blockvol.DeleteSnapshot(tempsnap)
	if err != nil {
		die("couldn't delete snapshot: %v", err)
	}
	fmt.Printf("copied %d bytes\n", size)
}

func getReaderFromArg(arg string) (*os.File, error) {
	return os.OpenFile(arg, os.O_RDONLY, 0)
}

func getWriterFromArg(arg string) (io.Writer, error) {
	if arg == "-" {
		return os.Stdout, nil
	}
	return os.Create(arg)
}

func loadAction(cmd *cobra.Command, args []string) {
	if len(args) != 2 && len(args) != 3 {
		cmd.Usage()
		os.Exit(1)
	}
	input, err := getReaderFromArg(args[0])
	if err != nil {
		die("couldn't open input: %v", err)
	}
	srv := createServer()
	defer srv.Close()
	fi, err := input.Stat()
	if err != nil {
		die("couldn't stat input file: %v", err)
	}

	size := uint64(fi.Size())
	if len(args) == 3 {
		expSize, err := humanize.ParseBytes(args[2])
		if err != nil {
			die("error parsing size %s: %v", args[2], err)
		}

		if expSize < size {
			die("size must be larger than input file")
		}

		size = expSize
	}

	err = block.CreateBlockVolume(srv.MDS, args[1], size)
	if err != nil {
		die("couldn't create block volume %s: %v", args[1], err)
	}
	blockvol, err := block.OpenBlockVolume(srv, args[1])
	if err != nil {
		die("couldn't open block volume %s: %v", args[1], err)
	}
	f, err := blockvol.OpenBlockFile()
	if err != nil {
		die("couldn't open blockfile %s: %v", args[1], err)
	}

	if progress {
		pb := new(progressutil.CopyProgressPrinter)
		pb.AddCopy(input, path.Base(args[0]), fi.Size(), f)
		err := pb.PrintAndWait(os.Stdout, 500*time.Millisecond, nil)
		if err != nil {
			die("couldn't copy: %v", err)
		}
	} else {
		_, err := io.Copy(f, input)
		if err != nil {
			die("couldn't copy: %v", err)
		}
	}

	err = f.Sync()
	if err != nil {
		die("couldn't sync: %v", err)
	}
	err = f.Close()
	if err != nil {
		die("couldn't close: %v", err)
	}
	fmt.Printf("copied %d bytes\n", fi.Size())
}
