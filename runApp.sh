
## Execution for Tmux 

cd EtherScan
echo "Executing Wallet Tracking ..!"
echo "Inside directory ${PWD}"
export exArr=("EtherScan" "ARBIScan" "BSCScan" "FTMScan" "MOVRScan" "PolygonScan" "SnowTraceScan")

for project in "${exArr[@]}"
do
   echo "Executing ${project}.."
   tmux new-session -d -s ${project} "./venv/bin/python WalletTracking.py ${project}"
done
echo "completed executing WalletTracking bot !"
echo ""

cd ../twitterFolloweeMonitor
echo "Executing twitter followee monitoring ..!"
echo "Inside directory ${PWD}"
tmux new-session -d -s TwitterMonitor "./venv/bin/python TwitterAPI_app.py"
echo "completed executing twitterFolloweeMonitor bot !"