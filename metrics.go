package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"time"
)

type Sample struct {
	Time    time.Time
	Latency time.Duration
}

type Bucket struct {
	Time    time.Time
	Dur     time.Duration
	Samples []Sample
}

var (
	bucketwindow  = time.Second
	processwindow = bucketwindow * 5
)

func processMetrics(filename string, data chan []Sample, errchan chan error) {
	bucketchan := make(chan Bucket)
	go func() {
		var (
			sorted bool
			buf    []Sample
			start  time.Time
			end    time.Time
		)

		for samples := range data {
			for _, sample := range samples {
				if sample.Time.After(end) {
					end = sample.Time
				} else {
					sorted = false
				}
				if start.IsZero() {
					start = sample.Time.Add(-bucketwindow)
				}
				if sample.Time.Before(start) {
					log.Println("Oh crap, sample outside of processwindow ", processwindow, "by", start.Sub(sample.Time))
					continue
				}
				buf = append(buf, sample)
			}

			for end.Sub(start) > processwindow+bucketwindow {
				if !sorted {
					sort.Slice(buf, func(a, b int) bool {
						return buf[a].Time.Before(buf[b].Time)
					})
					sorted = true
				}

				sendend := start.Add(bucketwindow)

				sendi := 0
				for i, v := range buf {
					if v.Time.After(sendend) {
						break
					}
					sendi = i
				}

				send := make([]Sample, sendi)
				copy(send, buf)
				bucketchan <- Bucket{
					Time:    start,
					Dur:     bucketwindow,
					Samples: send,
				}

				start = sendend
				copy(buf, buf[sendi:])
				buf = buf[0 : len(buf)-sendi]
			}
		}

		fmt.Println("metrics data closed, sending final values")

		// Send any final values
		sort.Slice(buf, func(a, b int) bool {
			return buf[a].Time.Before(buf[b].Time)
		})

		for len(buf) > 0 {
			sendend := start.Add(bucketwindow)

			fmt.Println("sending final [", start, ", ", sendend, "]")
			sendi := 0
			for _, v := range buf {
				if v.Time.After(sendend) {
					fmt.Println("breaking, v.Time is", v.Time)
					break
				}
				sendi++
			}
			//fmt.Println("sendi", sendi, "left", len(buf))
			//for i, v := range buf {
			//	fmt.Println(i, v.Time, v.Latency)
			//}

			send := make([]Sample, sendi)
			copy(send, buf)
			bucketchan <- Bucket{
				Time:    start,
				Dur:     bucketwindow,
				Samples: send,
			}

			start = sendend
			copy(buf, buf[sendi:])
			buf = buf[0 : len(buf)-sendi]
		}

		fmt.Println("closing bucketchan...")
		close(bucketchan)
	}()

	err := func() error {

		fh, err := os.Create(filename)
		if err != nil {
			return err
		}
		defer fh.Close()

		w := bufio.NewWriter(fh)

		var starttime time.Time

		fmt.Println("reading buckets...")
		for bucket := range bucketchan {
			if starttime.IsZero() {
				starttime = bucket.Time
			}

			samples := bucket.Samples

			row := ""

			if len(samples) == 0 {
				row = fmt.Sprintf("%f %f %f\n",
					bucket.Time.Sub(starttime).Seconds(),
					0,
					0,
				)
			} else {

				// sort the samples in the bucket by latency as opposed to time
				sort.Slice(samples, func(a, b int) bool {
					return samples[a].Latency < samples[b].Latency
				})

				limit99index := int(float64(len(samples)) * 0.9)
				limit99 := samples[limit99index].Latency

				sum := time.Duration(0)
				sum99 := time.Duration(0)
				count := len(samples)
				cnt99 := 0
				for _, sample := range samples {
					sum += sample.Latency

					if sample.Latency > limit99 {
						sum99 += sample.Latency
						cnt99++
					}
				}

				row = fmt.Sprintf("%f %f %f\n",
					bucket.Time.Sub(starttime).Seconds(),
					sum.Seconds()/float64(count),
					sum99.Seconds()/float64(cnt99),
				)
			}

			if _, err := w.WriteString(row); err != nil {
				return err
			}
		}
		fmt.Println("done reading buckets")

		if err := w.Flush(); err != nil {
			return err
		}
		if err := fh.Close(); err != nil {
			return err
		}

		fmt.Println("done!")
		return nil
	}()

	fmt.Println("metrics done, sending error", err)
	errchan <- err
}
