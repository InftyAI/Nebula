/*
Copyright 2026 The InftyAI Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package data holds the price catalog. The per-accelerator rows live in the CSVs
// (embedded by the parent package, so a price change is a reviewable data diff); this
// file holds the rates that are NOT per-accelerator, and so have no CSV row to sit on.
package data

// Modal meters CPU and memory SEPARATELY from the accelerator, so a sandbox's hourly
// cost is the GPU price PLUS these. Not universal: AWS bundles both into the instance
// price (p5.48xlarge's $98.320/hr already covers its vCPU and RAM), so a provider with
// no rates here is one whose CSV price is already all-in.
//
// Hourly, not per-second: Modal publishes $0.0000131/core/s and $0.00000222/GiB/s, but
// those are rounded renderings of the rates below (0.0473/3600 = 1.314e-5), and every
// price downstream of provider.Offering.PricePerHour is hourly.
//
// The GPU price is deliberately absent: it is a modal.csv row (H100 at $3.95 per GPU
// per hour), so each number keeps a single source of truth.
const (
	ModalCPUPricePerCoreHour   = 0.0473
	ModalMemoryPricePerGiBHour = 0.0080
)

// mibPerGiB converts a Pod's MiB request to the GiB the memory rate is quoted in.
const mibPerGiB = 1024

// ModalCPUCostPerHour and ModalMemoryCostPerHour are what Modal charges for a sandbox's
// CPU and memory, to be ADDED to its accelerator price. One function per published Modal
// rate, so each stays checkable against Modal's price page on its own.
//
// Each takes the unit the adapter already carries — fractional physical cores, and MiB
// (see modal.SandboxSpec) — so no conversion happens at the call site, which is where a
// factor-of-1024 slip would hide.
//
// Reservation, not usage: a sandbox bursting above its request toward CPULimit may bill
// above these.
func ModalCPUCostPerHour(cpuCores float64) float64 {
	return cpuCores * ModalCPUPricePerCoreHour
}

func ModalMemoryCostPerHour(memoryMiB int) float64 {
	return float64(memoryMiB) / mibPerGiB * ModalMemoryPricePerGiBHour
}
