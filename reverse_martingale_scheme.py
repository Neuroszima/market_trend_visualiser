from random import random
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure


# hard parameters
verbose = False
drawdown_management = True
simulation_number = 3000
trades = 3000
c = 0.45
# balance_history = [0]
# secondary_balance_history = [0]
reward_factor = 1.7
results_pool = []
start_operating_balance = 600
luck_parameter = 4

# soft params - need to start first
operating_balance = start_operating_balance
secondary_balance = 0
bet = 10
luck_in_row = 0
balance_history = [0]
secondary_balance_history = [0]


for strategy_case in range(simulation_number):
    balance_history = [0]
    secondary_balance_history = [0]
    operating_balance = start_operating_balance
    secondary_balance = 0
    luck_in_row = 0
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
            bet *= 2
            luck_in_row += 1
        else:
            operating_balance -= bet
            bet = 10
            luck_in_row = 0

        # money management strategy
        if luck_in_row > luck_parameter:
            if operating_balance > start_operating_balance:
                transfer = (operating_balance-start_operating_balance)*0.2
                operating_balance -= transfer
                secondary_balance += transfer
            bet = 10

        # feedback loop of balance management
        if drawdown_management and secondary_balance > 0:
            if operating_balance < 100:
                account_transfer = min([200, secondary_balance])
                secondary_balance -= account_transfer
                operating_balance += account_transfer

        # if bet > 100:
        #     print("target hit")
        #     secondary_balance += operating_balance - 50
        #     operating_balance = 50
        #     bet = 10
        balance_history.append(operating_balance)
        secondary_balance_history.append(secondary_balance)

    minimal_balance_state = min(balance_history)
    end_profit = secondary_balance_history[-1]

    # draw-down dependent variables
    can_make_money = operating_balance > 0 and secondary_balance > 0
    profitable_with_600_start = minimal_balance_state > -550
    profitable_with_1000_start = minimal_balance_state > -950
    results_pool.append((
        strategy_case,
        can_make_money,
        minimal_balance_state,
        end_profit,
        profitable_with_600_start,
        profitable_with_1000_start,
    ))


# worst scenario view
minimum_drawdown = min([sim[2] for sim in results_pool])

fig: Figure
axes: Axes
_, axes = plt.subplots()
axes.plot([i+1 for i in range(trades)], balance_history)
axes.plot([i+1 for i in range(trades)], secondary_balance_history)
axes.set_title(f"balance history reverse martingale c={c}")
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

results:
example: {results_pool[0]}
profitability probablity: {sum([int(sim[1]) for sim in results_pool])/simulation_number}
minimum drawdown: {minimum_drawdown}
maximum observed withdrawl end amount: {max([sim[3] for sim in results_pool])}

"""

# chance of being profitable with $600 start: {sum([sim[4] for sim in results_pool])/simulation_number}
# chance of being profitable with $1000 start: {sum([sim[5] for sim in results_pool])/simulation_number}

print(text_analisys)

with open(f'money_management_chance_{c}_reward_{reward_factor}_luck_{luck_parameter}'.replace('.', '_') + '.txt', 'w',
          encoding="UTF-8", newline='\n') as money_management_file:
    money_management_file.write(text_analisys)
