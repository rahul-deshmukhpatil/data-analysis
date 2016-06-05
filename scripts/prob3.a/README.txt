
# Single threaded mode run
./meanStdDev_byPart.py -f ../../data/dataLarge -r 1000000 > output

# Multi threaded mode run 
./meanStdDev_byPart.py -f ../../data/dataLarge -m -r 1000000 > output-mt
