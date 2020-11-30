
if test -f "stats/stats_"$1".txt"; then
	rm "stats/stats_"$1".txt"
fi
touch "stats/stats_"$1".txt"

python3 src/stats.py "logs/worker.log" "logs/master.log" $1 >> "stats/stats_"$1".txt"

echo "Statistics have been drawn. Please check the /stats folder for the plot and the txt file."
echo "File names to check are: "
echo "	- stats/stats_"$1".txt"
echo "	- stats/plot_"$1".png"
