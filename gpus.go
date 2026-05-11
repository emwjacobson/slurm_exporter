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
	partitions  []string
}

type nodeGPUData struct {
	partitions []string
	counts     map[string]float64 // gpu_type -> count
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

func ParseTotalGPUs() map[string]*nodeGPUData {
	gpu_map := make(map[string]*nodeGPUData)

	args := []string{"-h", "-o \"%n %R %G\""}
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
		if len(fields) < 3 {
			continue
		}
		nodeName := strings.Trim(fields[0], "\"")
		partition := strings.Trim(strings.TrimRight(fields[1], "*"), "\"")
		gres := strings.Trim(fields[2], "\"")

		nodeIsNew := false
		if _, seen := gpu_map[nodeName]; !seen {
			nodeIsNew = true
			gpu_map[nodeName] = &nodeGPUData{
				counts: make(map[string]float64),
			}
		}

		// GPU counts are a property of the node, not the partition — add once.
		if nodeIsNew {
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
					gpu_map[nodeName].counts[gpu_type] += count
				}
			}
		}

		// Track every distinct partition this node belongs to.
		partitionSeen := false
		for _, p := range gpu_map[nodeName].partitions {
			if p == partition {
				partitionSeen = true
				break
			}
		}
		if !partitionSeen {
			gpu_map[nodeName].partitions = append(gpu_map[nodeName].partitions, partition)
		}
	}

	return gpu_map
}

// slurm_gpus_alloc{type="k80",node="gpu01",partition="gpu"} 4
// slurm_gpus_idle{type="k80",node="gpu01",partition="gpu"} 20
// slurm_gpus_total{type="k80",node="gpu01",partition="gpu"} 24
// slurm_gpus_utilization{type="k80",node="gpu01",partition="gpu"} 0.16666
func ParseGPUsMetrics() map[string]map[string]*GPUsMetrics {
	metrics := make(map[string]map[string]*GPUsMetrics)

	totals := ParseTotalGPUs()
	alloc := ParseAllocatedGPUs()

	for node, nodeData := range totals {
		metrics[node] = make(map[string]*GPUsMetrics)
		for gpu_type, total := range nodeData.counts {
			allocCount := alloc[node][gpu_type]
			metrics[node][gpu_type] = &GPUsMetrics{
				alloc:       allocCount,
				idle:        total - allocCount,
				total:       total,
				utilization: allocCount / total,
				partitions:  nodeData.partitions,
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
	nodeLabels := []string{"type", "node"}
	partLabels := []string{"type", "partition"}

	return &GPUsCollector{
		alloc:            prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs by type and node", nodeLabels, nil),
		idle:             prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs by type and node", nodeLabels, nil),
		total:            prometheus.NewDesc("slurm_gpus_total", "Total GPUs by type and node", nodeLabels, nil),
		utilization:      prometheus.NewDesc("slurm_gpus_utilization", "GPU utilization by type and node", nodeLabels, nil),
		partitionAlloc:   prometheus.NewDesc("slurm_gpus_partition_alloc", "Allocated GPUs by type and partition", partLabels, nil),
		partitionIdle:    prometheus.NewDesc("slurm_gpus_partition_idle", "Idle GPUs by type and partition", partLabels, nil),
		partitionTotal:   prometheus.NewDesc("slurm_gpus_partition_total", "Total GPUs by type and partition", partLabels, nil),
		partitionUtil:    prometheus.NewDesc("slurm_gpus_partition_utilization", "GPU utilization by type and partition", partLabels, nil),
	}
}

type GPUsCollector struct {
	alloc          *prometheus.Desc
	idle           *prometheus.Desc
	total          *prometheus.Desc
	utilization    *prometheus.Desc
	partitionAlloc *prometheus.Desc
	partitionIdle  *prometheus.Desc
	partitionTotal *prometheus.Desc
	partitionUtil  *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.utilization
	ch <- cc.partitionAlloc
	ch <- cc.partitionIdle
	ch <- cc.partitionTotal
	ch <- cc.partitionUtil
}

func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	cm := GPUsGetMetrics()

	type partKey struct{ gpuType, partition string }
	type partTotals struct{ alloc, idle, total float64 }
	byPartition := make(map[partKey]*partTotals)

	for node, typeMap := range cm {
		for gpu_type, m := range typeMap {
			ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, m.alloc, gpu_type, node)
			ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, m.idle, gpu_type, node)
			ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, m.total, gpu_type, node)
			ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, m.utilization, gpu_type, node)

			for _, partition := range m.partitions {
				key := partKey{gpu_type, partition}
				if byPartition[key] == nil {
					byPartition[key] = &partTotals{}
				}
				byPartition[key].alloc += m.alloc
				byPartition[key].idle += m.idle
				byPartition[key].total += m.total
			}
		}
	}

	for key, pt := range byPartition {
		util := pt.alloc / pt.total
		ch <- prometheus.MustNewConstMetric(cc.partitionAlloc, prometheus.GaugeValue, pt.alloc, key.gpuType, key.partition)
		ch <- prometheus.MustNewConstMetric(cc.partitionIdle, prometheus.GaugeValue, pt.idle, key.gpuType, key.partition)
		ch <- prometheus.MustNewConstMetric(cc.partitionTotal, prometheus.GaugeValue, pt.total, key.gpuType, key.partition)
		ch <- prometheus.MustNewConstMetric(cc.partitionUtil, prometheus.GaugeValue, util, key.gpuType, key.partition)
	}
}
