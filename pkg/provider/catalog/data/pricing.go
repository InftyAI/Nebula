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
// Modal publishes these PER SECOND, so the literal stays exactly as printed on the price
// page and the scaling to an hour is left in the expression: the number a reviewer compares
// is the number Modal wrote, and the conversion is a compile-time constant fold. Hourly at
// all because every price downstream of provider.Offering.PricePerHour is.
//
// These are the SANDBOX/NOTEBOOK rates, ~3x Modal's standard Function rates ($0.0000131
// and $0.00000222 per second). The tier follows what we create, not what is cheapest: a
// NodeClaim becomes one Modal Sandbox (see modal.Client.CreateSandbox). Getting it wrong is
// nearly invisible on a GPU sandbox, where the accelerator dominates, and a 3x undercount
// on a CPU-only one, where these two rates are the whole bill.
//
// The GPU price is deliberately absent: it is a modal.csv row (H100 at $3.95 per GPU
// per hour), so each number keeps a single source of truth.
const (
	ModalCPUPricePerCoreHour   = 0.00003942 * 60 * 60
	ModalMemoryPricePerGiBHour = 0.00000667 * 60 * 60
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

// AWSGP3PricePerGBHour is gp3's US East (N. Virginia) $0.08/GB-month from
// aws.amazon.com/ebs/pricing (2026-10-04), spread over an average month. One rate for
// every region, since the catalog has no region axis (see provider.PriceRequest); other
// regions differ by a few cents per GB-month.
const AWSGP3PricePerGBHour = 0.08 / hoursPerMonth

// RunPod bundles a GPU Pod's vCPU and RAM into the GPU price, so its only extra on a GPU
// Pod is the container disk. A CPU-only Pod is priced per vCPU of its flavor, RAM included.
//
// RunPodCPU5cPricePerVCPUHour is cpu5c's `price.securePerVcpu` from GET /v2/catalog/cpus
// (2026-10-04); it must follow runpod's cpuFlavor. The disk rate is the pricing page's
// $0.10/GB/month for a running Pod, spread over an average month.
const (
	RunPodCPU5cPricePerVCPUHour       = 0.035
	RunPodContainerDiskPricePerGBHour = 0.10 / hoursPerMonth
)

// hoursPerMonth is 365 days / 12, the conversion for a rate quoted per month.
const hoursPerMonth = 730

// AWSRootVolumeCostPerHour is what EBS charges for a gp3 root volume of diskGiB, to be ADDED
// to the instance price, which covers no storage.
func AWSRootVolumeCostPerHour(diskGiB int) float64 {
	return float64(diskGiB) * AWSGP3PricePerGBHour
}

// RunPodCPUCostPerHour and RunPodContainerDiskCostPerHour take what the Pod is created
// with — the rounded vCPU count and disk size — not the Pod's raw request.
func RunPodCPUCostPerHour(vcpus int) float64 {
	return float64(vcpus) * RunPodCPU5cPricePerVCPUHour
}

func RunPodContainerDiskCostPerHour(diskGB int) float64 {
	return float64(diskGB) * RunPodContainerDiskPricePerGBHour
}
