cat temp/pid_to_kill.txt | while read in; do kill "$in"; done
