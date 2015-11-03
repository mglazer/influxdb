package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/influxdb/influxdb/tsdb"
	_ "github.com/influxdb/influxdb/tsdb/engine"
)

func main() {

	var inpath string
	var outpath string
	var cpuprofile string
	flag.StringVar(&inpath, "p", os.Getenv("HOME")+"/.influxdb", "Root storage path. [$HOME/.influxdb]")
	flag.StringVar(&outpath, "o", "", "output file name")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "name of CPU profile file to generate")
	flag.Parse()

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	tstore := tsdb.NewStore(filepath.Join(inpath, "data"))
	tstore.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	tstore.EngineOptions.Config.Dir = filepath.Join(inpath, "data")
	tstore.EngineOptions.Config.WALLoggingEnabled = false
	tstore.EngineOptions.Config.WALDir = filepath.Join(inpath, "wal")
	if err := tstore.Open(); err != nil {
		fmt.Printf("Failed to open dir: %v\n", err)
		os.Exit(1)
	}

	var w io.Writer = os.Stdout
	var err error

	if outpath != "" {
		// Open or create the output file for writing.
		w, err = os.OpenFile(outpath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
		if err != nil {
			fmt.Printf("Failed to open: %s\n", err)
			os.Exit(1)
		}
		defer w.(*os.File).Close()
		// Create a compressed writer if the file extention is .gz.
		if path.Ext(outpath) == ".gz" {
			w = gzip.NewWriter(w)
			if err != nil {
				fmt.Printf("Failed to create gzip writer: %s\n", err)
				os.Exit(1)
			}
		}
	}

	size, err := tstore.DiskSize()
	if err != nil {
		fmt.Printf("Failed to determine disk usage: %v\n", err)
	}

	// Summary stats
	fmt.Fprintf(w, "# Shards: %d, Indexes: %d, Databases: %d, Disk Size: %d, Series: %d\n",
		tstore.ShardN(), tstore.DatabaseIndexN(), len(tstore.Databases()), size, countSeries(tstore))
	fmt.Fprintln(w, "#")

	tw := tabwriter.NewWriter(w, 16, 8, 0, '\t', 0)

	fmt.Fprintln(tw, strings.Join([]string{"# Shard", "DB", "Measurement", "Tags [#K/#V]", "Fields [Name:Type]", "Series"}, "\t"))

	shardIDs := tstore.ShardIDs()

	databases := tstore.Databases()
	sort.Strings(databases)

	for _, db := range databases {
		index := tstore.DatabaseIndex(db)
		measurements := index.Measurements()
		sort.Sort(measurements)
		for _, m := range measurements {
			tags := m.TagKeys()
			tagValues := 0
			for _, tag := range tags {
				tagValues += len(m.TagValues(tag))
			}
			fields := m.FieldNames()
			sort.Strings(fields)
			series := m.SeriesKeys()
			sort.Strings(series)
			sort.Sort(ShardIDs(shardIDs))

			// Sample a point from each measurement to determine the field types
			for _, shardID := range shardIDs {
				shard := tstore.Shard(shardID)
				tx, err := shard.ReadOnlyTx()
				if err != nil {
					fmt.Printf("Failed to get transaction: %v", err)
				}

				for _, key := range series {
					fieldSummary := []string{}

					cursor := tx.Cursor(key, tsdb.Forward)

					// Series doesn't exist in this shard
					if cursor == nil {
						continue
					}

					// Seek to the beginning
					_, value := cursor.Seek([]byte{})
					codec := shard.FieldCodec(m.Name)
					if codec != nil {
						fields, err := codec.DecodeFieldsWithNames(value)
						if err != nil {
							fmt.Printf("Failed to decode values: %v", err)
						}

						for field, value := range fields {
							fieldSummary = append(fieldSummary, fmt.Sprintf("%s:%T", field, value))
						}
						sort.Strings(fieldSummary)
					}
					fmt.Fprintf(tw, "# %d\t%s\t%s\t%d/%d\t%d [%s]\t%d\n", shardID, db, m.Name, len(tags), tagValues,
						len(fields), strings.Join(fieldSummary, ","), len(series))
					break
				}
				tx.Rollback()
			}
		}
	}
	tw.Flush()
	if err := dumpData(tstore, w); err != nil {
		fmt.Println(err)
	}
}

func countSeries(tstore *tsdb.Store) int {
	var count int
	for _, shardID := range tstore.ShardIDs() {
		shard := tstore.Shard(shardID)
		cnt, err := shard.SeriesCount()
		if err != nil {
			fmt.Printf("series count failed: %v\n", err)
			continue
		}
		count += cnt
	}
	return count
}

func dumpData(tstore *tsdb.Store, w io.Writer) error {
	w = bufio.NewWriterSize(w, 32000000)
	defer w.(*bufio.Writer).Flush()
	shardIDs := tstore.ShardIDs()

	databases := tstore.Databases()
	sort.Strings(databases)

	fmt.Fprintln(w, "# DML")
	for _, db := range databases {
		fmt.Fprintf(w, "# CONTEXT-DATABASE:%s\n", db)
		fmt.Fprintf(w, "# CONTEXT-RETENTION-POLICY:default\n")
		index := tstore.DatabaseIndex(db)
		measurements := index.Measurements()
		sort.Sort(measurements)
		for _, m := range measurements {
			tags := m.TagKeys()
			tagValues := 0
			for _, tag := range tags {
				tagValues += len(m.TagValues(tag))
			}
			fields := m.FieldNames()
			sort.Strings(fields)
			series := m.SeriesKeys()
			sort.Strings(series)
			sort.Sort(ShardIDs(shardIDs))

			// Sample a point from each measurement to determine the field types
			for _, shardID := range shardIDs {
				shard := tstore.Shard(shardID)
				tx, err := shard.ReadOnlyTx()
				if err != nil {
					fmt.Printf("Failed to get transaction: %v", err)
				}

				for _, key := range series {
					cursor := tx.Cursor(key, tsdb.Forward)

					// Series doesn't exist in this shard
					if cursor == nil {
						continue
					}

					// Seek to the beginning
					codec := shard.FieldCodec(m.Name)
					if codec != nil {
						for ts, value := cursor.Seek([]byte{}); value != nil; ts, value = cursor.Next() {
							fieldSummary := []string{}
							fields, err := codec.DecodeFieldsWithNames(value)
							if err != nil {
								fmt.Printf("Failed to decode values: %v", err)
							}

							for field, value := range fields {
								fieldSummary = append(fieldSummary, fmt.Sprintf("%s=%v", field, value))
							}
							fmt.Fprintf(w, "%s %s %d\n", key, strings.Join(fieldSummary, ","), int64(btou64(ts)))
						}
					}
				}
				tx.Rollback()
			}
		}
	}
	//q, err := influxql.ParseQuery("select * from /.*/")
	//if err != nil {
	//	return err
	//}
	//for _, shardID := range tstore.ShardIDs() {
	//	m, err := tstore.CreateMapper(shardID, q.Statements[0], 10000)
	//	if err != nil {
	//		return err
	//	}

	//	c, err := m.NextChunk()
	//	if err != nil {
	//		return err
	//	}

	//	spew.Dump(c)
	//}

	return nil
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

type ShardIDs []uint64

func (a ShardIDs) Len() int           { return len(a) }
func (a ShardIDs) Less(i, j int) bool { return a[i] < a[j] }
func (a ShardIDs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
