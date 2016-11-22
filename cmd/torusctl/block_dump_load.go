package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/coreos/torus"
	"github.com/coreos/torus/block"

	"github.com/coreos/pkg/progressutil"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

var (
	blockDumpCommand = &cobra.Command{
		Use:   "dump VOLUME OUTPUT_FILE",
		Short: "dump the contents of a block volume to an output file",
		Run: func(cmd *cobra.Command, args []string) {
			err := blockDumpAction(cmd, args)
			if err == torus.ErrUsage {
				cmd.Usage()
				os.Exit(1)
			} else if err != nil {
				die("%v", err)
			}
		},
	}

	blockLoadCommand = &cobra.Command{
		Use:   "load INPUT_FILE VOLUME [SIZE]",
		Short: "load the contents of a file into a new block volume",
		Run: func(cmd *cobra.Command, args []string) {
			err := blockLoadAction(cmd, args)
			if err == torus.ErrUsage {
				cmd.Usage()
				os.Exit(1)
			} else if err != nil {
				die("%v", err)
			}
		},
	}

	progress bool
)

func init() {
	blockDumpCommand.Flags().BoolVarP(&progress, "progress", "p", false, "show progress")
	blockCommand.AddCommand(blockDumpCommand)
	blockLoadCommand.Flags().BoolVarP(&progress, "progress", "p", false, "show progress")
	blockCommand.AddCommand(blockLoadCommand)
}

func blockDumpAction(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return torus.ErrUsage
	}
	output, err := getWriterFromArg(args[1])
	if err != nil {
		return fmt.Errorf("couldn't open output: %v", err)
	}
	srv := createServer()
	defer srv.Close()
	blockvol, err := block.OpenBlockVolume(srv, args[0])
	if err != nil {
		return fmt.Errorf("couldn't open block volume %s: %v", args[0], err)
	}
	tempsnap := fmt.Sprintf("temp-dump-%d", os.Getpid())
	err = blockvol.SaveSnapshot(tempsnap)
	if err != nil {
		return fmt.Errorf("couldn't snapshot: %v", err)
	}
	bf, err := blockvol.OpenSnapshot(tempsnap)
	if err != nil {
		return fmt.Errorf("couldn't open snapshot: %v", err)
	}

	size := int64(bf.Size())
	if progress {
		pb := progressutil.NewCopyProgressPrinter()
		pb.AddCopy(bf, path.Base(args[0]), size, output)
		err := pb.PrintAndWait(os.Stderr, 500*time.Millisecond, nil)
		if err != nil {
			return fmt.Errorf("couldn't copy: %v", err)
		}
	} else {
		n, err := io.Copy(output, bf)
		if err != nil {
			return fmt.Errorf("couldn't copy: %v", err)
		}

		if n != size {
			return fmt.Errorf("short read of %q", args[0])
		}
	}

	err = blockvol.DeleteSnapshot(tempsnap)
	if err != nil {
		return fmt.Errorf("couldn't delete snapshot: %v", err)
	}
	fmt.Printf("copied %d bytes\n", size)
	return nil
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

func blockLoadAction(cmd *cobra.Command, args []string) error {
	if len(args) != 2 && len(args) != 3 {
		return torus.ErrUsage
	}
	input, err := getReaderFromArg(args[0])
	if err != nil {
		return fmt.Errorf("couldn't open input: %v", err)
	}
	srv := createServer()
	defer srv.Close()
	fi, err := input.Stat()
	if err != nil {
		return fmt.Errorf("couldn't stat input file: %v", err)
	}

	size := uint64(fi.Size())
	if len(args) == 3 {
		expSize, err := humanize.ParseBytes(args[2])
		if err != nil {
			return fmt.Errorf("error parsing size %s: %v", args[2], err)
		}

		if expSize < size {
			return fmt.Errorf("size must be larger than input file")
		}

		size = expSize
	}

	err = block.CreateBlockVolume(srv.MDS, args[1], size)
	if err != nil {
		return fmt.Errorf("couldn't create block volume %s: %v", args[1], err)
	}
	blockvol, err := block.OpenBlockVolume(srv, args[1])
	if err != nil {
		return fmt.Errorf("couldn't open block volume %s: %v", args[1], err)
	}
	f, err := blockvol.OpenBlockFile()
	if err != nil {
		return fmt.Errorf("couldn't open blockfile %s: %v", args[1], err)
	}
	defer f.Close()

	if progress {
		pb := progressutil.NewCopyProgressPrinter()
		pb.AddCopy(input, path.Base(args[0]), fi.Size(), f)
		err := pb.PrintAndWait(os.Stderr, 500*time.Millisecond, nil)
		if err != nil {
			return fmt.Errorf("couldn't copy: %v", err)
		}
	} else {
		_, err := io.Copy(f, input)
		if err != nil {
			return fmt.Errorf("couldn't copy: %v", err)
		}
	}

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("couldn't sync: %v", err)
	}
	fmt.Printf("copied %d bytes\n", fi.Size())
	return nil
}
