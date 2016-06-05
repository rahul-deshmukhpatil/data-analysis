
# Single threaded mode run
./meanStdDev_part_multithreaded.py -f ../../data/dataLarge -r 1000000 > output

# Multi threaded mode run 
./meanStdDev_part_multithreaded.py -f ../../data/dataLarge -m -r 1000000 > output-mt
