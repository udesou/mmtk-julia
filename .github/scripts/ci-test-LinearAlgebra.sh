# Running LinearAlgebra as a separate item
# Given it takes on average more than 2.5h to run 

set -e

. $(dirname "$0")/common.sh

export MMTK_MAX_HSIZE_G=4
total_mem=$(free -m | awk '/^Mem:/ {print $2}')
num_workers=3
export JULIA_TEST_MAXRSS_MB=$((total_mem/ num_workers))

echo "-> Run single threaded"
ci_run_jl_test "LinearAlgebra" 3