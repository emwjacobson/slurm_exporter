/* Copyright 2020 Joeri Hermans, Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"io/ioutil"
	"os/exec"
	"strings"
	"strconv"
)

type GPUsMetrics struct {
	alloc       float64
	idle        float64
	total       float64
	utilization float64
}

// Returns map of [node][gpu_type]GPUsMetrics
func GPUsGetMetrics() map[string]map[string]*GPUsMetrics {
	return ParseGPUsMetrics()
}

// expandNodes expands a Slurm nodelist expression (e.g. "gpu[01-03]") to
// individual hostnames using scontrol. Falls back to treating the raw string
// as a single node on error.
func expandNodes(nodelist string) []string {
	cmd := exec.Command("scontrol", "show", "hostname", nodelist)
	out, err := cmd.Output()
	if err != nil || len(out) == 0 {
		return []string{nodelist}
	}
	nodes := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(nodes) == 0 {
		return []string{nodelist}
	}
	return nodes
}

func ParseAllocatedGPUs() map[string]map[string]float64 {
	gpu_map := make(map[string]map[string]float64)

	args := []string{"--state=RUNNING", "--noheader", "--Format=tres-alloc:.,nodelist:."}
	output := string(Execute("squeue", args))

	if len(output) == 0 {
		return gpu_map
	}

	for _, line := range strings.Split(output, "\n") {
		if len(line) == 0 {
			continue
		}

		// billing=30,cpu=1,gres/gpu:a100=2,gres/gpu=2,mem=100G,node=1  gpu[01-02]
		line = strings.Trim(line, "\"")
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		tres := strings.Trim(fields[0], "\"")
		nodelist := strings.Trim(fields[1], "\"")

		nodes := expandNodes(nodelist)
		numNodes := float64(len(nodes))

		for _, resource := range strings.Split(tres, ",") {
			if strings.HasPrefix(resource, "gres/gpu:") {
				descriptor := strings.TrimPrefix(resource, "gres/gpu:")
				values := strings.Split(descriptor, "=")
				if len(values) < 2 {
					continue
				}
				gpu_type := values[0]
				count, _ := strconv.ParseFloat(values[1], 64)
				perNode := count / numNodes

				for _, node := range nodes {
					if gpu_map[node] == nil {
						gpu_map[node] = make(map[string]float64)
					}
					gpu_map[node][gpu_type] += perNode
				}
			}
		}
	}

	return gpu_map
}

func ParseTotalGPUs() map[string]map[string]float64 {
	gpu_map := make(map[string]map[string]float64)

	args := []string{"-h", "-o \"%n %G\""}
	output := string(Execute("sinfo", args))

	if len(output) == 0 {
		return gpu_map
	}

	for _, line := range strings.Split(output, "\n") {
		if len(line) == 0 {
			continue
		}

		line = strings.Trim(line, "\"")
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		nodeName := strings.Trim(fields[0], "\"")
		gres := strings.Trim(fields[1], "\"")

		for _, resource := range strings.Split(gres, ",") {
			if strings.HasPrefix(resource, "gpu:") {
				// format: gpu:<type>:N(S:<something>), e.g. gpu:RTX2070:2(S:0)
				parts := strings.Split(resource, ":")
				if len(parts) < 3 {
					continue
				}
				gpu_type := parts[1]
				descriptor := strings.Split(parts[2], "(")[0]
				count, _ := strconv.ParseFloat(descriptor, 64)

				if gpu_map[nodeName] == nil {
					gpu_map[nodeName] = make(map[string]float64)
				}
				gpu_map[nodeName][gpu_type] += count
			}
		}
	}

	return gpu_map
}

// slurm_gpus_alloc{type="k80",node="gpu01"} 4
// slurm_gpus_idle{type="k80",node="gpu01"} 20
// slurm_gpus_total{type="k80",node="gpu01"} 24
// slurm_gpus_utilization{type="k80",node="gpu01"} 0.16666
func ParseGPUsMetrics() map[string]map[string]*GPUsMetrics {
	metrics := make(map[string]map[string]*GPUsMetrics)

	totals := ParseTotalGPUs()
	alloc := ParseAllocatedGPUs()

	for node, typeMap := range totals {
		metrics[node] = make(map[string]*GPUsMetrics)
		for gpu_type, total := range typeMap {
			allocCount := alloc[node][gpu_type]
			metrics[node][gpu_type] = &GPUsMetrics{
				alloc:       allocCount,
				idle:        total - allocCount,
				total:       total,
				utilization: allocCount / total,
			}
		}
	}

	return metrics
}

// Execute the sinfo command and return its output
func Execute(command string, arguments []string) []byte {
	cmd := exec.Command(command, arguments...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	labels := []string{"type", "node"}

	return &GPUsCollector{
		alloc:       prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs by type", labels, nil),
		idle:        prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs by type", labels, nil),
		total:       prometheus.NewDesc("slurm_gpus_total", "Total GPUs by type", labels, nil),
		utilization: prometheus.NewDesc("slurm_gpus_utilization", "Total GPU utilization by type", labels, nil),
	}
}

type GPUsCollector struct {
	alloc       *prometheus.Desc
	idle        *prometheus.Desc
	total       *prometheus.Desc
	utilization *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.utilization
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	cm := GPUsGetMetrics()
	for node, typeMap := range cm {
		for gpu_type, m := range typeMap {
			ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, m.alloc, gpu_type, node)
			ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, m.idle, gpu_type, node)
			ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, m.total, gpu_type, node)
			ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, m.utilization, gpu_type, node)
		}
	}
}
