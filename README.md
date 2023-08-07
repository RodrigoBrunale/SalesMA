
# :chart_with_upwards_trend: Sales Moving Average Analyzer (SalesMA)

![Moving averages are fun!](https://placekitten.com/900/300)

Sales Moving Averages (MA) are like the bread and butter of the financial world :moneybag:, but did you know they're also the secret sauce :secret: to analyzing and predicting sales data for your brick-and-mortar business? That's right! Your local store is not so different from the NYSE, after all :department_store:.

## :question: What is a Moving Average?

A Moving Average (MA) is a widely used indicator in technical analysis that helps smooth out price action by filtering out the "noise" from random price fluctuations. It is a trend-following—or lagging—indicator because it is based on past prices.

The two basic and commonly used MAs are the simple moving average (SMA), which is the simple average of a security over a defined number of time periods, and the exponential moving average (EMA), which gives more weight to recent prices. The most common applications of MAs are to identify trend direction and to determine support and resistance levels.

## :dart: How Can Moving Averages Help with Managing Sales?

In the context of a retail business, a moving average of sales data can help identify trends and seasonal patterns. This can inform decisions about when to order new stock, when to ramp up staffing levels, or when to run promotional campaigns. 

In short, applying moving averages to your sales data can give you a clearer picture of your business's performance and future :crystal_ball:.

## :exclamation: Assumptions

The script makes the following assumptions:

1. Your sales data is stored in Google BigQuery :heavy_check_mark:.
2. Your sales data includes a date for each sale and a value for each sale. The name of these columns should be specified in the `config.yaml` file :date: :dollar:.
3. Each sale is recorded as a separate entry in your data :receipt:.
4. The destination for the moving average tables will also be BigQuery :heavy_check_mark:.
5. You have Python installed on your system and are somewhat comfortable using the command line :snake: :computer:.
6. You have the necessary permissions to read from your sales data source and write to your destination in BigQuery. The authentication is done either using the `GOOGLE_APPLICATION_CREDENTIALS` environment variable or using the service account credentials specified in the `config.yaml` file :key:.

Please ensure these assumptions hold true for your case for the script to work correctly :memo:.

## :rocket: How to Use SalesMA

1. Clone this repo to your local machine using `https://github.com/<your_username>/SalesMA.git` (make sure to replace `<your_username>` with your actual GitHub username).

2. Navigate to the directory where you cloned the repo :file_folder:.

3. Install the Python dependencies by running `pip install -r requirements.txt` :gear:.

4. Run the script using `python main.py`. If a `config.yaml` file doesn't exist, the script will create a template for you and then exit :pencil:.

5. Open the `config.yaml` file and set the configuration parameters according to your BigQuery setup and specific needs :wrench:.

6. Run the script again using `python main.py` :rocket:.

7. Sit back, relax, and let SalesMA do the heavy lifting. Once the script finishes, your moving averages will be sitting pretty in your specified BigQuery tables :sunglasses:.

8. Go make data-driven decisions and watch your business thrive :tada:!

## :warning: Disclaimer

While moving averages can be a helpful tool in your decision-making process, they should not be used in isolation. Always consider other factors and metrics when making significant business decisions. Also, this script won't make coffee. We're really sorry about that :coffee:.

## :scroll: License

MIT License. See the `LICENSE` file for details.
