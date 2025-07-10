package cmd

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aybabtme/uniplot/histogram"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/jmoiron/sqlx"
	"github.com/negrel/assert"
	"github.com/rodaine/table"
	"github.com/urfave/cli/v3"
	"golang.org/x/term"

	"github.com/zperf/fusestream/histo"
)

var statCommand = &cli.Command{
	Name: "stat",
	Commands: []*cli.Command{
		statSummaryCommand,
		statExportCsvCommand,
	},
}

var tableHeaderFmt = color.New(color.FgGreen, color.Underline).SprintfFunc()
var tableColumnFmt = color.New(color.FgYellow).SprintfFunc()

var consoleWidth int

func printLine() {
	if consoleWidth == 0 {
		width, _, err := term.GetSize(0)
		if err != nil {
			panic(err)
		}
		consoleWidth = width
	}

	fmt.Println(strings.Repeat("-", consoleWidth))
}

type printOp struct {
	Name string
	Fn   func() error
}

var zeroTime = time.Time{}

var statExportCsvCommand = &cli.Command{
	Name: "export-csv",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "dsn",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "output",
			Aliases:  []string{"o"},
			Required: true},
		&cli.BoolFlag{
			Name:    "humanize",
			Aliases: []string{"hh"},
		},
		&cli.StringFlag{
			Name:  "start",
			Value: "0001-01-01T00:00:00Z",
		},
		&cli.StringFlag{
			Name:  "end",
			Value: "0001-01-01T00:00:00Z",
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		outputPath := command.String("output")
		dsn := command.String("dsn")
		isHumanize := command.Bool("humanize")

		startTs, err := time.Parse(time.RFC3339, command.String("start"))
		if err != nil {
			return err
		}

		endTs, err := time.Parse(time.RFC3339, command.String("end"))
		if err != nil {
			return err
		}

		db, err := sqlx.Open("duckdb", dsn)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		if !isHumanize {
			startTimeNs := startTs.UnixNano()
			endTimeNs := endTs.UnixNano()

			if startTs != zeroTime && endTs != zeroTime {
				_, err = db.Exec(fmt.Sprintf(
					`COPY (SELECT start_time_ns, name, elapsed_ns, "offset", length, path FROM slowio_records
WHERE %v <= start_time_ns AND start_time_ns <= %v)
TO '%s' (HEADER, DELIMITER ',')`, startTimeNs, endTimeNs, outputPath))
			} else if startTs != zeroTime && endTs == zeroTime {
				_, err = db.Exec(fmt.Sprintf(
					`COPY (SELECT start_time_ns, name, elapsed_ns, "offset", length, path FROM slowio_records
WHERE %v <= start_time_ns)
TO '%s' (HEADER, DELIMITER ',')`, startTimeNs, outputPath))
			} else if startTs == zeroTime && endTs != zeroTime {
				_, err = db.Exec(fmt.Sprintf(
					`COPY (SELECT start_time_ns, name, elapsed_ns, "offset", length, path FROM slowio_records
WHERE start_time_ns <= %v)
TO '%s' (HEADER, DELIMITER ',')`, endTimeNs, outputPath))
			} else if startTs == zeroTime && endTs == zeroTime {
				_, err = db.Exec(fmt.Sprintf(
					`COPY (SELECT start_time_ns, name, elapsed_ns, "offset", length, path FROM slowio_records)
TO '%s' (HEADER, DELIMITER ',')`, outputPath))
			}
			if err != nil {
				return err
			}
		} else {
			f, err := os.Create(outputPath)
			if err != nil {
				return err
			}
			w := csv.NewWriter(f)
			defer func() {
				w.Flush()
				_ = f.Close()
			}()

			err = w.Write([]string{"start", "name", "elapsed", "offset", "length", "path"})
			if err != nil {
				return err
			}

			rows, err := db.Query(`SELECT name, start_time_ns, elapsed_ns, "offset", length, path FROM slowio_records`)
			if err != nil {
				return err
			}

			for rows.Next() {
				var name, path string
				var startTimeNs, elapsedNs, offset int64
				var length int32
				err = rows.Scan(&name, &startTimeNs, &elapsedNs, &offset, &length, &path)
				if err != nil {
					return err
				}

				startTime := time.Unix(0, startTimeNs).Local()

				writeFn := func() error {
					return w.Write([]string{
						startTime.Format(time.RFC3339Nano),
						name,
						(time.Duration(elapsedNs) * time.Nanosecond).String(),
						fmt.Sprintf("%d", offset),
						fmt.Sprintf("%d", length),
						path,
					})
				}

				err = nil
				if startTs != zeroTime && endTs != zeroTime {
					if startTime.After(startTs) && startTime.Before(endTs) {
						err = writeFn()
					}
				} else if startTs != zeroTime && startTime.After(startTs) {
					err = writeFn()
				} else if endTs != zeroTime && startTime.Before(endTs) {
					err = writeFn()
				} else if startTs == zeroTime && endTs == zeroTime {
					err = writeFn()
				}

				if err != nil {
					return err
				}
			}
			err = rows.Err()
			if err != nil {
				return err
			}
		}

		return nil
	},
}

var statSummaryCommand = &cli.Command{
	Name: "summary",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "dsn",
			Required: true,
		},
	},
	Action: func(ctx context.Context, command *cli.Command) error {
		dsn := command.String("dsn")
		db, err := sqlx.Open("duckdb", dsn)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		s := NewFuseStat(db, os.Stdout)

		printFuncTable := []printOp{
			{"Summary", s.PrintSummary},
			{"Random/Sequential ratio", s.PrintRandomSequentialRatioTable},
			{"File system operation count histogram", s.PrintFileSystemOperationHistogram},
			{"File system operation durations (unit: ms)", s.PrintFileSystemOperationElapseBarChart},
			{"I/O size histogram", s.PrintIOSizeHistogram},
		}

		for _, p := range printFuncTable {
			if p.Name != "Summary" {
				fmt.Println(p.Name)
			}

			err = p.Fn()
			if err != nil {
				return err
			}

			printLine()
		}

		return nil
	},
}

type FuseStat struct {
	w  io.Writer
	db *sqlx.DB
}

func NewFuseStat(db *sqlx.DB, w io.Writer) *FuseStat {
	return &FuseStat{
		w:  w,
		db: db,
	}
}

func (s *FuseStat) PrintSummary() error {
	var meanElapsed, maxElapsed, minElapsed float64
	err := s.db.QueryRow("SELECT MEAN(elapsed_ns), MAX(elapsed_ns), MIN(elapsed_ns) FROM slowio_records;").
		Scan(&meanElapsed, &maxElapsed, &minElapsed)
	if err != nil {
		return err
	}

	var runtime float64
	err = s.db.QueryRow(`SELECT (MAX(start_time_ns) - MIN(start_time_ns) +
        (SELECT elapsed_ns FROM slowio_records ORDER BY start_time_ns DESC LIMIT 1)) / 1000000 / 1000
	FROM slowio_records;`).Scan(&runtime)
	if err != nil {
		return err
	}

	var reads, writes int64
	err = s.db.QueryRow(`SELECT COUNT(*) FROM slowio_records WHERE name = 'fuse.Read';`).Scan(&reads)
	if err != nil {
		return err
	}

	err = s.db.QueryRow(`SELECT COUNT(*) FROM slowio_records WHERE name = 'fuse.Write';`).Scan(&writes)
	if err != nil {
		return err
	}

	rwRatio := 1.0
	if writes > 0 {
		rwRatio = float64(reads) / float64(writes)
	}

	var totalBytes int64
	err = s.db.QueryRow(`SELECT SUM(length) FROM slowio_records
                   WHERE name = 'fuse.Read' OR name = 'fuse.Write';`).Scan(&totalBytes)
	if err != nil {
		return err
	}

	var ioCount int64
	err = s.db.QueryRow(`SELECT COUNT(*) FROM slowio_records
                   WHERE name = 'fuse.Read' OR name = 'fuse.Write';`).Scan(&ioCount)
	if err != nil {
		return err
	}

	var meanIOSize float64
	var maxIOSize, minIOSize int64
	err = s.db.QueryRow(`SELECT MEAN(length), MAX(length), MIN(length) FROM slowio_records
                    WHERE name = 'fuse.Read' OR name = 'fuse.Write';`).Scan(&meanIOSize, &maxIOSize, &minIOSize)
	if err != nil {
		return err
	}

	tbl := table.New("Property", "Mean", "Min", "Max").WithWriter(s.w).
		WithHeaderFormatter(tableHeaderFmt).WithFirstColumnFormatter(tableColumnFmt)
	tbl.AddRow("Runtime", fmt.Sprintf("%.3fs", runtime))
	tbl.AddRow("Bandwidth", fmt.Sprintf("%s/s", humanize.Bytes(uint64(float64(totalBytes)/runtime))))
	tbl.AddRow("IOPS", fmt.Sprintf("%.3f", float64(ioCount)/runtime))
	tbl.AddRow("R/W ratio", fmt.Sprintf("%.3f", rwRatio))
	tbl.AddRow("I/O size",
		fmt.Sprintf("%v", humanize.Bytes(uint64(meanIOSize))),
		fmt.Sprintf("%v", humanize.Bytes(uint64(minIOSize))),
		fmt.Sprintf("%v", humanize.Bytes(uint64(maxIOSize))),
	)
	tbl.AddRow("I/O elapsed",
		fmt.Sprintf("%.3fms", meanElapsed/1000/1000),
		fmt.Sprintf("%.3fms", minElapsed/1000/1000),
		fmt.Sprintf("%.3fms", maxElapsed/1000/1000),
	)

	tbl.Print()
	return nil
}

func (s *FuseStat) PrintFileSystemOperationElapseBarChart() error {
	data := make(map[string]float64)

	rows, err := s.db.Query("SELECT name, elapsed_ns FROM slowio_records;")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var name string
		var elapsedNs int64
		err = rows.Scan(&name, &elapsedNs)
		if err != nil {
			return err
		}
		data[name] += float64(elapsedNs) / 1000 / 1000 // ms
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return histo.PrintBarChart(data, s.w)
}

func (s *FuseStat) PrintFileSystemOperationHistogram() error {
	names := make([]string, 0)

	rows, err := s.db.Query(`SELECT name FROM slowio_records;`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return err
		}
		names = append(names, name)
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return histo.PrintHistogram(names, s.w)
}

func (s *FuseStat) PrintIOSizeHistogram() error {
	data := make([]float64, 0)

	rows, err := s.db.Query("SELECT length FROM slowio_records;")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var length int64
		err = rows.Scan(&length)
		if err != nil {
			return err
		}
		data = append(data, float64(length))
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	h := histogram.Hist(256/16, data)
	return histo.SkipZeroPrintf(s.w, h, histogram.Linear(histo.MaxWidth), func(v float64) string {
		return fmt.Sprintf("%vKiB", int(v/1024))
	})
}

func (s *FuseStat) PrintRandomSequentialRatioTable() error {
	tbl := table.New("Path", "Random(%)", "Sequential(%)").
		WithWriter(s.w).
		WithHeaderFormatter(tableHeaderFmt).
		WithFirstColumnFormatter(tableColumnFmt)

	files := make([]string, 0)
	rows, err := s.db.Query(`SELECT DISTINCT path FROM slowio_records
		WHERE name = 'fuse.Read' OR name = 'fuse.Write'
		ORDER BY path;`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var path string
		err = rows.Scan(&path)
		if err != nil {
			return err
		}
		files = append(files, path)
	}

	for _, path := range files {
		var rnd, seq int64
		rnd, seq, err = s.countRandomIOs(path)
		if err != nil {
			return err
		}
		cnt := rnd + seq

		tbl.AddRow(path,
			fmt.Sprintf("%.3f", float64(rnd)/float64(cnt)*100),
			fmt.Sprintf("%.3f", float64(seq)/float64(cnt)*100),
		)
	}

	tbl.Print()
	return nil
}

type ioOperation struct {
	Op         string
	LastOffset int64
}

func (op *ioOperation) Empty() bool {
	return len(op.Op) == 0 && op.LastOffset == 0
}

func (op *ioOperation) Advance(name string, offset int64, length int64) bool {
	isSeq := true

	if op.Op != name {
		isSeq = false
		op.Op = name
	}

	if op.LastOffset != offset {
		isSeq = false
	}

	op.LastOffset = offset + length

	return isSeq
}

func (s *FuseStat) countRandomIOs(path string) (rnd int64, seq int64, err error) {
	rnd = 0
	seq = 0
	err = nil

	rows, err := s.db.Query(`SELECT name, "offset", length FROM slowio_records
         WHERE path = ? AND (name = 'fuse.Read' OR name = 'fuse.Write')
         ORDER BY start_time_ns;`, path)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	ioState := ioOperation{}
	cnt := int64(0)

	for rows.Next() {
		var name string
		var offset int64
		var length int32

		err = rows.Scan(&name, &offset, &length)
		if err != nil {
			return
		}

		if ioState.Empty() {
			ioState = ioOperation{name, offset + int64(length)}
			// I don't think the first I/O is sequential
		} else {
			if ioState.Advance(name, offset, int64(length)) {
				seq++
			} else {
				rnd++
			}
			cnt++
		}
	}

	err = rows.Err()
	if err != nil {
		return
	}

	assert.True(cnt == seq+rnd)
	return
}
