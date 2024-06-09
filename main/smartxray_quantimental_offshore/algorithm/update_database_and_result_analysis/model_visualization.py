import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go

def xgboost_shap_figure(is_offshore=False, model_name='categorized_xgboost_1', horizons=['1', '3', '6', '12']):

    if is_offshore:
        model_data_path = addpath.offshore_standardized_model_data_path
    else:
        model_data_path = addpath.onshore_standardized_model_data_path

    model_path = os.path.join(model_data_path, "Xgboost", "Output", model_name)
    save_path = os.path.join(model_path, "SHAP")
    if os.path.exists(save_path):
        pass
    else:
        os.makedirs(save_path)
    for hrz in horizons:
        for idx in ['Equity_Large', 'Equity_Mid', 'Bond', 'Money', 'Balance', 'QD']:
            shap_value = pd.read_csv(os.path.join(model_path, hrz + "_" + idx + "_shap.csv"), parse_dates=['date'], index_col=0)
            shap_value = shap_value.set_index(['date', 'ms_secid'])
            f_list = pd.read_csv(os.path.join(model_path, hrz + "_" + idx + ".csv"))
            f_list = f_list['factor'].tolist()[-25:]
            colrow = 5
            fig = make_subplots(rows=colrow, cols=colrow, start_cell="bottom-left")
            for i in range(len(f_list)):
                x = shap_value[f_list[i]]
                y = shap_value[f_list[i] + "_shap"]
                row = int(i / colrow) + 1
                col = i + 1 - (row - 1) * colrow
                fig.add_trace(go.Scatter(x=x, y=y, name=f_list[i], mode='markers'), row=row, col=col)
            fig.update_layout(title=hrz + "_" + idx + "_SHAP", plot_bgcolor='ghostwhite')
            output_path = os.path.join(save_path, hrz + "_" + idx + '_shap.html')
            fig.write_html(output_path)