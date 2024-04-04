from typing import Callable

import vis_functions.data_visualiser as data_visualizer
import vis_functions.vis_helpers as vis_helpers


PriceChart: type = data_visualizer.PriceChart

rgb_to_matlab: Callable = vis_helpers.rgb_to_matlab_
AVAILABLE_THEMES = vis_helpers.AVAILABLE_THEMES_
resolve_timeframe_name: Callable = vis_helpers.resolve_timeframe_name_
