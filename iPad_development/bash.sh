
cd "/root/development/EAZ/iPad_development/fetchClientAlphaData"
/usr/local/bin/pm2 start "fetchClientAlphaData.py" --interpreter="/root/development/AkashLiveAlgosBacktests/venv/bin/python3" --name="fetchClientAlphaData-1-1" --no-autorestart --time
