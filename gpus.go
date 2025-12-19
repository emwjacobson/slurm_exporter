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

// Returns map of ["gpu_type"]GPUsMetrics
func GPUsGetMetrics() map[string]*GPUsMetrics {
	return ParseGPUsMetrics()
}

func ParseAllocatedGPUs() map[string]float64 {
	gpu_map := make(map[string]float64)

	// squeue --state RUNNING --noheader --Format=tres-alloc:.
	args := []string{"--state=RUNNING", "--noheader", "--Format=tres-alloc:."}
	output := string(Execute("squeue", args))
	//args := []string{"-a", "-X", "--format=AllocTRES", "--state=RUNNING", "--noheader", "--parsable2"}
	//output := string(Execute("sacct", args))

	if len(output) == 0 {
		return make(map[string]float64)
	}

	for _, line := range strings.Split(output, "\n") {
		if len(line) == 0 {
			continue
		}
		
		// billing=30,cpu=1,gres/gpu:a100=2,gres/gpu=2,mem=100G,node=1
		line = strings.Trim(line, "\"")
		for _, resource := range strings.Split(line, ",") {
			if strings.HasPrefix(resource, "gres/gpu:") { // Look for specific GPU type, eg "gres/gpu:k80=1"
				descriptor := strings.TrimPrefix(resource, "gres/gpu:") // k80=1
				values := strings.Split(descriptor, "=")
				gpu_type := values[0]
				count, _ := strconv.ParseFloat(values[1], 64)
				
				gpu_map[gpu_type] += count
			}
		}
	}

	return gpu_map
}

func ParseTotalGPUs() map[string]float64 {
	gpu_map := make(map[string]float64)

	args := []string{"-h", "-o \"%n %G\""}
	output := string(Execute("sinfo", args))

	if len(output) == 0 {
		return make(map[string]float64)
	}

	for _, line := range strings.Split(output, "\n") {
		if len(line) == 0 {
			continue
		}

		line = strings.Trim(line, "\"")
		gres := strings.Fields(line)[1]
		// gres column format: comma-delimited list of resources
		for _, resource := range strings.Split(gres, ",") {
			if strings.HasPrefix(resource, "gpu:") {
				// format: gpu:<type>:N(S:<something>), e.g. gpu:RTX2070:2(S:0)
				descriptor := strings.Split(resource, ":")[2] // 2(S:0)
				descriptor = strings.Split(descriptor, "(")[0] // 2
				node_gpus, _ :=  strconv.ParseFloat(descriptor, 64)

				type_gpu := strings.Split(resource, ":")[1] // RTX2070
				gpu_map[type_gpu] += node_gpus
			}
		}
	}

	return gpu_map
}


// slurm_gpus_alloc{type="k80"} 4
// slurm_gpus_alloc{type="a100"} 20
// ...
// slurm_gpus_idle{type="k80"} 20 (calculated value = total-alloc)
// slurm_gpus_idle{type="a100"} 4
// ...
// slurm_gpus_total{type="k80"} 24
// slurm_gpus_total{type="a100"} 24
// ...
// slurm_gpus_utilization{type="k80"} = 0.16666 (calculated value = alloc/total)
// slurm_gpus_utilization{type="a100"} = 0.83333
func ParseGPUsMetrics() map[string]*GPUsMetrics {
	types := make(map[string]*GPUsMetrics)

	totals := ParseTotalGPUs()
	alloc := ParseAllocatedGPUs()

	// TODO: Make sure keys in totals and alloc are the same

	for gpu_type := range totals {
		types[gpu_type] = &GPUsMetrics{0, 0, 0, 0}

		types[gpu_type].alloc = alloc[gpu_type]
		types[gpu_type].total = totals[gpu_type]
		types[gpu_type].idle = totals[gpu_type] - alloc[gpu_type]
		types[gpu_type].utilization = alloc[gpu_type] / totals[gpu_type]
	}

	return types
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
	labels := []string{"type"}

	return &GPUsCollector{
		alloc: prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs by type", labels, nil),
		idle:  prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs by type", labels, nil),
		total: prometheus.NewDesc("slurm_gpus_total", "Total GPUs by type", labels, nil),
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
	for gpu_type := range cm {
		ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, float64(cm[gpu_type].alloc), gpu_type)
		ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, float64(cm[gpu_type].idle), gpu_type)
		ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, float64(cm[gpu_type].total), gpu_type)
		ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, float64(cm[gpu_type].utilization), gpu_type)
	}
}
