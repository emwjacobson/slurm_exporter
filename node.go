/* Copyright 2021 Chris Read

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
	"log"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// NodeMetrics stores metrics for each node
type NodeMetrics struct {
	cpuAlloc uint64
	cpuIdle  uint64
	cpuOther uint64
	cpuTotal uint64

	memAlloc uint64
	memTotal uint64

	gpuAlloc uint64

	hasGPU bool
	gpuType string
	gpuIndex []int

	nodeStatus string
}

func NodeGetMetrics() map[string]*NodeMetrics {
	return ParseNodeMetrics(NodeData())
}

// ParseNodeMetrics takes the output of sinfo with node data
// It returns a map of metrics per node
func ParseNodeMetrics(input []byte) map[string]*NodeMetrics {
	nodes := make(map[string]*NodeMetrics)
	lines := strings.Split(string(input), "\n")

	// Sort and remove all the duplicates from the 'sinfo' output
	sort.Strings(lines)
	linesUniq := RemoveDuplicates(lines)

	for _, line := range linesUniq {
		node := strings.Fields(line)
		nodeName := node[0]
		nodes[nodeName] = &NodeMetrics{0, 0, 0, 0, 0, 0, 0, false, "", nil, ""}


		// Status Info
		nodes[nodeName].nodeStatus = node[4] // mixed, allocated, etc.


		// Memory Info
		memAlloc, _ := strconv.ParseUint(node[1], 10, 64)
		memTotal, _ := strconv.ParseUint(node[2], 10, 64)

		nodes[nodeName].memAlloc = memAlloc
		nodes[nodeName].memTotal = memTotal


		// CPU Info
		cpuInfo := strings.Split(node[3], "/")
		cpuAlloc, _ := strconv.ParseUint(cpuInfo[0], 10, 64)
		cpuIdle, _ := strconv.ParseUint(cpuInfo[1], 10, 64)
		cpuOther, _ := strconv.ParseUint(cpuInfo[2], 10, 64)
		cpuTotal, _ := strconv.ParseUint(cpuInfo[3], 10, 64)

		nodes[nodeName].cpuAlloc = cpuAlloc
		nodes[nodeName].cpuIdle = cpuIdle
		nodes[nodeName].cpuOther = cpuOther
		nodes[nodeName].cpuTotal = cpuTotal


		// GPU Info
		gpuTotalStr := node[5] // "gpu:a100:8" or "(null)" if no GPUs
		gpuAllocStr := node[6] // "gpu:a100:6(IDX:0,2-6)" - multiple, non-contiguous
							  // "gpu:a100:6(IDX:0,2-3,6)" - multiple, non-contiguous
							  // "gpu:a100:8(IDX:0-7)" - multiple, contiguous
							  // "gpu:ada6000:1(IDX:0)" - single
							  // "gpu:k80:0(IDX:N/A)" - none
		
		if (gpuTotalStr != "(null)") { // Has GPU
			nodes[nodeName].hasGPU = true
			gpu_str := strings.Split(gpuAllocStr, "(")
			usedGPUs := strings.Split(gpu_str[0], ":") // gpu:a100:6
			nodes[nodeName].gpuType = usedGPUs[1]

			nodes[nodeName].gpuAlloc, _ = strconv.ParseUint(usedGPUs[2], 10, 64)
			num_gpus, _ := strconv.ParseUint(strings.Split(gpuTotalStr, ":")[2], 10, 64)

			// index_list = IDX:0,2-6
						 // IDX:0,2-3,6
						 // IDX:0-7
						 // IDX:0
						 // IDX:N/A
			index_list := strings.TrimSuffix(gpu_str[1], ")")
			index_list = strings.Split(index_list, ":")[1]

			nodes[nodeName].gpuIndex = make([]int, num_gpus)
			if (index_list != "N/A") {
				for _, part := range strings.Split(index_list, ",") {
					if strings.Contains(part, "-") {
						// Range
						bounds := strings.Split(part, "-")
						start, _ := strconv.Atoi(bounds[0])
						end, _ := strconv.Atoi(bounds[1])
						for i := start; i <= end; i++ {
							nodes[nodeName].gpuIndex[i] = 1
						}
					} else {
						// Single Digit
						num, _ := strconv.Atoi(part)
						nodes[nodeName].gpuIndex[num] = 1
					}
				}
			}
		}
	}

	return nodes
}

// NodeData executes the sinfo command to get data for each node
// It returns the output of the sinfo command
func NodeData() []byte {
	cmd := exec.Command("sinfo", "-h", "-N", "-O", "NodeList,AllocMem,Memory,CPUsState,StateLong,Gres,GresUsed:.")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

type NodeCollector struct {
	cpuAlloc *prometheus.Desc
	cpuIdle  *prometheus.Desc
	cpuOther *prometheus.Desc
	cpuTotal *prometheus.Desc

	memAlloc *prometheus.Desc
	memTotal *prometheus.Desc

	gpuAlloc *prometheus.Desc
}

// NewNodeCollector creates a Prometheus collector to keep all our stats in
// It returns a set of collections for consumption
func NewNodeCollector() *NodeCollector {
	labels_cpu := []string{"node","status"}
	labels_gpu := []string{"node","type","index"}

	return &NodeCollector{
		cpuAlloc: prometheus.NewDesc("slurm_node_cpu_alloc", "Allocated CPUs per node", labels_cpu, nil),
		cpuIdle:  prometheus.NewDesc("slurm_node_cpu_idle", "Idle CPUs per node", labels_cpu, nil),
		cpuOther: prometheus.NewDesc("slurm_node_cpu_other", "Other CPUs per node", labels_cpu, nil),
		cpuTotal: prometheus.NewDesc("slurm_node_cpu_total", "Total CPUs per node", labels_cpu, nil),
		
		memAlloc: prometheus.NewDesc("slurm_node_mem_alloc", "Allocated memory per node", labels_cpu, nil),
		memTotal: prometheus.NewDesc("slurm_node_mem_total", "Total memory per node", labels_cpu, nil),

		gpuAlloc: prometheus.NewDesc("slurm_node_gpu_alloc", "Allocated GPUs per node", labels_gpu, nil),
	}
}

// Send all metric descriptions
func (nc *NodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.cpuAlloc
	ch <- nc.cpuIdle
	ch <- nc.cpuOther
	ch <- nc.cpuTotal

	ch <- nc.memAlloc
	ch <- nc.memTotal

	ch <- nc.gpuAlloc
}

func (nc *NodeCollector) Collect(ch chan<- prometheus.Metric) {
	nodes := NodeGetMetrics()
	for node := range nodes {
		ch <- prometheus.MustNewConstMetric(nc.cpuAlloc, prometheus.GaugeValue, float64(nodes[node].cpuAlloc), node, nodes[node].nodeStatus)
		ch <- prometheus.MustNewConstMetric(nc.cpuIdle,  prometheus.GaugeValue, float64(nodes[node].cpuIdle),  node, nodes[node].nodeStatus)
		ch <- prometheus.MustNewConstMetric(nc.cpuOther, prometheus.GaugeValue, float64(nodes[node].cpuOther), node, nodes[node].nodeStatus)
		ch <- prometheus.MustNewConstMetric(nc.cpuTotal, prometheus.GaugeValue, float64(nodes[node].cpuTotal), node, nodes[node].nodeStatus)

		ch <- prometheus.MustNewConstMetric(nc.memAlloc, prometheus.GaugeValue, float64(nodes[node].memAlloc), node, nodes[node].nodeStatus)
		ch <- prometheus.MustNewConstMetric(nc.memTotal, prometheus.GaugeValue, float64(nodes[node].memTotal), node, nodes[node].nodeStatus)

		if (nodes[node].hasGPU) {
			for i := range nodes[node].gpuIndex {
				ch <- prometheus.MustNewConstMetric(nc.gpuAlloc, prometheus.GaugeValue, float64(nodes[node].gpuIndex[i]), node, nodes[node].gpuType, strconv.Itoa(i))
			}
		}
	}
}
