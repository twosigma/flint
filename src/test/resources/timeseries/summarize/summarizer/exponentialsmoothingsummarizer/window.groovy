// This is how results in window.csv are generated

import com.twosigma.dcat.functions.groovyland.Summary.EMAConvention
import com.twosigma.dcat.functions.groovyland.Summary.EMAType

table = readCSV('window.csv')
table = keepColumns(table, ['time', 'v'])

windowedTable  = addPastWindows(table, [window: Window.absoluteTime("1 day")])
summarizers = [
  Summary.emaHalfLife("v","60minute", EMAType.PREVIOUS_POINT, EMAConvention.CORE),
  Summary.emaHalfLife("v","60minute", EMAType.CURRENT_POINT, EMAConvention.CORE),
  Summary.emaHalfLife("v","60minute", EMAType.LINEAR_INTERPOLATION, EMAConvention.CORE),
  Summary.emaHalfLife("v","60minute", EMAType.PREVIOUS_POINT, EMAConvention.CONVOLUTION),
  Summary.emaHalfLife("v","60minute", EMAType.CURRENT_POINT, EMAConvention.CONVOLUTION),
  Summary.emaHalfLife("v","60minute", EMAType.LINEAR_INTERPOLATION, EMAConvention.CONVOLUTION),
  Summary.emaHalfLife("v","60minute", EMAType.PREVIOUS_POINT, EMAConvention.LEGACY),
  Summary.emaHalfLife("v","60minute", EMAType.CURRENT_POINT, EMAConvention.LEGACY),
  Summary.emaHalfLife("v","60minute", EMAType.LINEAR_INTERPOLATION, EMAConvention.LEGACY),
]

summarized = summarizeWindows(windowedTable, "window", summarizers)

summarized = deleteColumns(summarized, 'window')
summarized = deleteColumns(summarized, ~/.*propPrimed.*/)

summarized = renameColumns(
  summarized,
  ["window_v_ema60minuteHalfLife": "expected_core_previous",
   "window_v_ema60minuteHalfLife2": "expected_core_current",
   "window_v_ema60minuteHalfLife3": "expected_core_linear",
   "window_v_ema60minuteHalfLife4": "expected_convolution_previous",
   "window_v_ema60minuteHalfLife5": "expected_convolution_current",
   "window_v_ema60minuteHalfLife6": "expected_convolution_linear",
   "window_v_ema60minuteHalfLife7": "expected_legacy_previous",
   "window_v_ema60minuteHalfLife8": "expected_legacy_current",
   "window_v_ema60minuteHalfLife9": "expected_legacy_linear",
  ]
)

preview(summarized, 30)