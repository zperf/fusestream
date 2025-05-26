package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aybabtme/uniplot/histogram"
	"github.com/fatih/color"
	"github.com/jmoiron/sqlx"
	"github.com/rodaine/table"
	"github.com/urfave/cli/v3"
	"golang.org/x/term"

	"github.com/fanyang89/slowio/histo"
)

var statCommand = &cli.Command{
	Name: "stat",
	Commands: []*cli.Command{
		statSummaryCommand,
	},
}

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
	headerFmt := color.New(color.FgGreen, color.Underline).SprintfFunc()
	columnFmt := color.New(color.FgYellow).SprintfFunc()

	tbl := table.New("Property", "Value").
		WithWriter(s.w).
		WithHeaderFormatter(headerFmt).
		WithFirstColumnFormatter(columnFmt)

	var cnt int
	var meanElapsed, maxElapsed, minElapsed float64
	err := s.db.QueryRow("SELECT COUNT(*), MEAN(elapsed_ns), MAX(elapsed_ns), MIN(elapsed_ns) FROM slowio_records;").
		Scan(&cnt, &meanElapsed, &maxElapsed, &minElapsed)
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

	tbl.AddRow("Record count", cnt)
	tbl.AddRow("Runtime", fmt.Sprintf("%.3fs", runtime))
	tbl.AddRow("Mean operation runtime", fmt.Sprintf("%.3fms", meanElapsed/1000/1000))
	tbl.AddRow("Min operation runtime", fmt.Sprintf("%.3fms", minElapsed/1000/1000))
	tbl.AddRow("Max operation runtime", fmt.Sprintf("%.3fms", maxElapsed/1000/1000))
	tbl.AddRow("R/W ratio", fmt.Sprintf("%.3f", rwRatio))

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
