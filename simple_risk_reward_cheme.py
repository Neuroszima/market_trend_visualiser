from random import random
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure


# hard params
verbose = False
simulation_number = 3000
trades = 3000
c = 0.42
reward_factor = 2
results_pool = []
start_operating_balance = 800
comissions = 1.74

# soft params - need to start first
operating_balance = start_operating_balance
secondary_balance = 0
bet = 8
luck_in_row = 0
balance_history = [start_operating_balance]

# accumulating plot
fig: Figure
axes: Axes
_, axes = plt.subplots()

for strategy_case in range(simulation_number):
    balance_history = [start_operating_balance]
    # secondary_balance_history = [0]
    operating_balance = start_operating_balance
    # secondary_balance = 0
    bet = 10

    if not (strategy_case % 50):
        print(strategy_case, ' processing')
    for i in range(trades-1):
        toss = random() < c

        if verbose:
            print(f"prediction {toss}")
            print(f"{operating_balance=}")
            print(f"{bet=}")
            print(f"{secondary_balance=}")

        # trade results and success counting
        if toss:
            operating_balance += bet * reward_factor
        else:
            operating_balance -= bet

        # commissions
        operating_balance -= comissions

        balance_history.append(operating_balance)

    minimal_balance_state = min(balance_history)
    end_balance = balance_history[-1]

    # draw-down dependent variables
    can_make_money = operating_balance > start_operating_balance  # and secondary_balance > 0
    profitable_with_600_start = minimal_balance_state > -550
    profitable_with_1000_start = minimal_balance_state > -950
    results_pool.append((
        strategy_case,
        can_make_money,
        minimal_balance_state,  # drawdown
        # end_profit,
        end_balance,
        profitable_with_600_start,
        profitable_with_1000_start,
    ))

axes.plot([i + 1 for i in range(trades)], balance_history)

# strategy points of interest view
minimum_drawdown = min([sim[2] for sim in results_pool])
strategy_expected_profit = sum([sim[3] for sim in results_pool])/simulation_number
interest_rate = strategy_expected_profit/start_operating_balance

axes.plot([0, 2999], [start_operating_balance, strategy_expected_profit])

# axes.plot([i+1 for i in range(trades)], secondary_balance_history)
axes.set_title(f"balance history c={c}, reward_factor={reward_factor}")  # luck={luck_parameter}
axes.set_xlabel("trade number")
axes.set_ylabel("account balance")
plt.show()


text_analisys = f"""
money management analysis:

inputs:
number of simulation retries = {simulation_number}
trades per "simulation" = {trades}
chance of single success = {round(c, 2)}
reward factor = {reward_factor}
start operating balance = {start_operating_balance}

results:
example: {results_pool[0]}
profitability probablity: {sum([int(sim[1]) for sim in results_pool])/simulation_number}
minimum drawdown: {minimum_drawdown}
strategy expectancy ($ end profit): {strategy_expected_profit}
strategy expectancy (x interest rate): {interest_rate}
"""

print(text_analisys)

with open(f'simple_management_chance_{c}_reward_{reward_factor}'.replace('.', '_') + '.txt', 'w',
          encoding="UTF-8", newline='\n') as money_management_file:
    money_management_file.write(text_analisys)
