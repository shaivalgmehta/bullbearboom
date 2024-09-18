##### TO SET UP TIMESCALE DB ####
1. Run setup_db_py

##### TRANFORM STOCK DATA #####
1. data_transformer.py script is used to transform data from various sources

#### GET STOCK DATA #####
1. stock_data_fetcher.py is used to fetch stock data from various sources


#### RUNNING THE APPLICATION ####
1. In Digital Ocean, run the script ./start_app.sh

OR
-----------
Manually:

echo "Starting the backend (Flask app)..."
cd /path/to/backend
source venv/bin/activate
pkill -f "python app.py"  # Kill any existing instance
nohup python app.py > app.log 2>&1 &
echo "Backend started. Check app.log for details."

echo "Starting the frontend (React app)..."
cd /path/to/frontend
npm run build
pkill -f "npx serve -s build"  # Kill any existing instance
nohup npx serve -s build -l 3000 > frontend.log 2>&1 &
echo "Frontend started. Check frontend.log for details."

echo "Application startup complete."