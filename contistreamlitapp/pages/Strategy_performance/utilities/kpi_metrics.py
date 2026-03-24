import numpy as np 
import pandas as pd 
import json
from sklearn.metrics import confusion_matrix

class Kpi:
	"""
	Calculate Kpi from the strategy 
	Parameters:
    ----------
        Data (pd.DataFrame): Pandas dataframe with Backtest result it must contain
							- Direction
							- Spread
							- True direction
							- Probability (Optional)
							
    Output:
        Strategy statistics (Dictionary) : Dictionary with main statistics of the strategy
    """

	def __init__(self,data, strategy, configuration):

		self.data = data
 
		self.fees = configuration[strategy]["fee"]

		self.settings_strat = configuration[strategy]

	def _calculate_metrics_for_stance(self,result_data, stance):
		
		kpi_dict = dict()

		if stance != "undecided":
			kpi_dict["Net Profit"] = round(result_data.loc[result_data["Profit"]>0, "Profit"].sum())
			kpi_dict["Net Loss"] = round(result_data.loc[result_data["Profit"]< 0, "Profit"].sum())
			kpi_dict["Net PnL"] = round(result_data["Profit"].sum(), 2)

			if kpi_dict["Net Loss"] == 0:
				kpi_dict["Profit Factor"] = np.nan
			else:
				kpi_dict["Profit Factor"] = round(kpi_dict["Net Profit"] / kpi_dict["Net Loss"], 2)
			kpi_dict["Potential"] = round(result_data["Potential"].sum(), 2)

			wins = sum(np.sign(result_data["Profit"]) > 0)
			losses = sum(np.sign(result_data["Profit"]) < 0)

			kpi_dict["Hit Rate"] = round(wins / (wins + losses), 4) * 100
			kpi_dict["Number of Winning Trades"] = wins
			kpi_dict["Number of Losing Trades"] = losses
			kpi_dict["Total Number of Trades"] = wins + losses

			kpi_dict["Average GBP/MWh"] = round(kpi_dict["Net PnL"] /((result_data['Volume']*result_data["Direction"].abs()).sum()), 2)

		kpi_dict["Potential"] = result_data["Potential"].sum()
		if stance in ["all", "undecided"]:
			kpi_dict["Undecided Stances"] = sum(result_data["Direction"] == 0)

		if stance == "all":

			result_data.index = result_data.index.tz_convert('CET')
			Max_daily_loss = result_data["Profit"].resample('D').sum().min()
			Max_daily_gain = result_data["Profit"].resample('D').sum().max()
			
			max_winning_days, max_loosing_days = self._streaks(result_data)

			kpi_dict["Max Consecutive Winning Days"] = max_winning_days
			kpi_dict["Max Consecutive Losing Days"] = max_loosing_days
			kpi_dict["Max Daily Draw Down"] = Max_daily_loss
			kpi_dict["Max Daily Win"] = Max_daily_gain
			# kpi_dict['Sharpe ratio'] = self._sharpe_ratio(result_data)
			kpi_dict['VAR'] = self._var(result_data)
			kpi_dict['Std'] = result_data['Spread'].std()

			values = result_data["Direction"].value_counts()
			try:
				kpi_dict["Time in Market"] = 100 - round(values[0] / len(result_data["Direction"]) * 100, 2)
			except KeyError:
				kpi_dict["Time in Market"] = 100

		return kpi_dict

	def _streaks(self,result_data):
		
		result_data_day = result_data.resample('D').sum()
		result_data_day["win"] = result_data_day["Profit"]
		result_data_day.loc[result_data_day["win"] > 0, "win"] = 1
		result_data_day.loc[result_data_day["win"] < 0, "win"] = -1

		grouper = (result_data_day.win != result_data_day.win.shift()).cumsum()
		result_data_day['streak'] = result_data_day["win"].groupby(grouper).cumsum()
		max_winning_days = result_data_day['streak'].max()
		max_loosing_days = result_data_day['streak'].min()

		return max_winning_days,max_loosing_days 


	def _heat_map(self, results):

		profit_hour = results["Profit"].groupby(results.index.hour).sum()
		profit_dayofweek = results["Profit"].groupby(results.index.day_name()).sum()
		profit_day_hour = results[["Profit", "Spread"]].groupby(by =[results.index.dayofweek,results.index.hour]).sum()
		profit_heat_map = profit_day_hour["Profit"].unstack()
		profit_heat_map = profit_heat_map.round(0)

		return profit_hour, profit_dayofweek, profit_heat_map
	
	def _confusion_matrix(self, result_data):

		cm = confusion_matrix(result_data["TrueDirection"].values, result_data["Direction"].values, normalize= 'true')
		cm = cm.round(2)
		cm = cm*100
		
		return cm


	def _fat_tails(self, result_data):
		
		skewness = result_data.skew()
		kurtosis = result_data.kurtosis()
		p99 = result_data.quantile(0.9999) 
		p1	= result_data.quantile(0.0001)
		fat_tails = pd.DataFrame(data = [skewness, kurtosis, p99, p1], index = ["skewness","kurtosis","p99","p1"])

		return fat_tails
	
	def _liquidity(self,auction_data):
		
		monthly_volume = auction_data.groupby(by = auction_data.index.month).mean()
		hourly_volume = auction_data.groupby(by = auction_data.index.hour).mean()
		
		weekly = auction_data.groupby(by = [auction_data.index.dayofweek,auction_data.index.hour]).mean()
		vol_heat_map = weekly.unstack()
		vol_heat_map = vol_heat_map.round(0)

		monthly_hourly = auction_data.groupby(by = [auction_data.index.month,auction_data.index.hour]).mean()
		monthly_hourly_heatmap = monthly_hourly.unstack()
		monthly_hourly_heatmap = monthly_hourly_heatmap.round(0)


		# agregated curves 

		return hourly_volume, monthly_volume, vol_heat_map, monthly_hourly_heatmap

	def _sharpe_ratio(self,result_data):


		daily_return = result_data["Profit"].groupby(result_data.index.date).sum()
		periods = len(daily_return)**0.5
		Sharpe_ratio = periods*daily_return.mean()/np.std(daily_return)

		return Sharpe_ratio
	
	def _sortino_ratio(self, result_data):
		"""
		Calculate the Sortino Ratio for a dataset of returns.
		"""
		daily_return = result_data["Profit"].groupby(result_data.index.date).sum()
		periods = len(daily_return)**0.5

		risk_free_rate = 0 # Example: 2% annual risk-free rate

		# Calculate the excess returns
		excess_return = daily_return - risk_free_rate
		# Calculate downside deviation (only consider negative returns)
		downside_deviation = (daily_return[daily_return < risk_free_rate] ** 2).mean() ** 0.5

		# Calculate the Sortino Ratio
		Sortino_ratio = periods* excess_return.mean() / downside_deviation

		return Sortino_ratio
	
	def _calmar_ratio(self, result_data):

		annualized_return = np.mean(result_data['Profit']) * 365  # Assuming 252 trading days in a year
		rolling_max = result_data['Profit'].cummax()
		daily_drawdown = result_data['Profit']/rolling_max - 1
		max_drawdown = daily_drawdown.cummin().min()

		# Calmar Ratio
		calmar_ratio = annualized_return / abs(max_drawdown)

		return calmar_ratio
	
	def _var(self, result_data):
		"""
		empirical VaR for 95% confidence level
		calculate daily VAR
		""" 
		trades = result_data.loc[result_data['Direction'].abs() !=0]
		trades_daily = trades.groupby(trades.index.tz_convert('Europe/Paris').date)['Profit'].sum()
		var_95 = trades_daily.reset_index().quantile(0.05)
		var_95 = var_95.values[1]

		return var_95
	
	def _ROI(self, result_data, collateral, vol):

		ROI = 100*result_data['cum_pnl'][-1]/(collateral*vol)

		return ROI
	

	def calculate_kpi_metrics(self):
		
		keys = ['Net Profit', 'Net Loss', 'Net PnL', 'Profit Factor', 'Potential', 'Hit Rate', 'Number of Winning Trades', 'Number of Losing Trades', 
		'Total Number of Trades', 'Average GBP/MWh', 'Undecided Stances', 'Max Consecutive Winning Days', 'Max Consecutive Losing Days', 
		'Max Daily Draw Down', 'Max Daily Win', 'Std','Time in Market']
		result_data = self.data
		result_data["Potential"] = result_data["Spread"].abs() - self.fees

		shorts = result_data[result_data["Direction"] == -1].copy()
		longs = result_data[result_data["Direction"] == 1].copy()
		undecided = result_data[result_data["Direction"] == 0].copy()

		stats = self._calculate_metrics_for_stance(result_data, "all")
		
		if len(shorts) == 0:

			short_stats = {key : np.nan for key in keys}
		else:
			short_stats = self._calculate_metrics_for_stance(shorts, "short")
		
		if len(longs) == 0:

			long_stats = {key : np.nan for key in keys}
		else:
			long_stats = self._calculate_metrics_for_stance(longs, "long")
		
		undecided_stats = self._calculate_metrics_for_stance(undecided, "undecided")

		statistics = pd.DataFrame([stats, short_stats, long_stats]).round(2).T 
		statistics.columns = ["All", "Short", "Long"] 

		long_df = result_data.loc[result_data['Direction']==1]
		
		short_df = result_data.loc[result_data['Direction']==-1]

		profit_hour, profit_dayofweek, profit_heat_map = self._heat_map(result_data)
		long_profit_hour, long_profit_dayofweek, long_profit_heat_map = self._heat_map(long_df)
		short_profit_hour, short_profit_dayofweek, short_profit_heat_map = self._heat_map(short_df)

		fat_tails = self._fat_tails(result_data)
		Sharpe_ratio = self._fat_tails(result_data)
		var = self._var(result_data)

		ROI = self._ROI(result_data, self.settings_strat['collateral_cost_mwh'], self.settings_strat['volume'])
		confusion_matrix = self._fat_tails(result_data)

		statistics.loc['ROI', 'All'] = ROI
		statistics = statistics.reset_index()
		statistics = statistics.rename(columns = {'index': 'Key Performance Metrics'})


		kpi_metrics = {
						"statistics": statistics,
						"VAR ": var,
						"ROI on Collaretal": ROI,
						"fat_tails": fat_tails,
						"profit hourly average": {"profit hour":profit_hour,
												"profit hour long":long_profit_hour,
												"profit hour short":short_profit_hour},
						"profit_daily average": {"profit day": profit_dayofweek,
												"profit day long": long_profit_dayofweek,
												"profit day short":short_profit_dayofweek},
						"profit heat map": {"profit_heat map total": profit_heat_map,
											"profit_heat_map_long": long_profit_heat_map,
											"profit_heat_map_short": short_profit_heat_map},
						"confusion matrix": confusion_matrix
						}

		return kpi_metrics

