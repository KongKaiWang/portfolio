import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from scipy import stats


if __name__ == '__main__':
    DIRECTORY = r'C:\Users\KW38770\Documents\WangK\Formation\Impingement\Analysis\Compiled Data'
    FILE = r'COMPILED_DATA_MISSING.csv'

    df = pd.read_csv(rf'{DIRECTORY}\{FILE}')
    # Remove outliers (past 99.7% percentile)
    df = df[(df['IR'] < 15) & (df['CC1'] < 1255)]
    # df = df[(np.abs(stats.zscore(df[['CC1', 'CC2', 'IR', 'JRD_AVG', 'FIRST_CC', 'FIRST_CV', 'FIRST_CAP', 'FIRST_OCV', 'OCV', 'CCV', 'NORM_DV']])) < 3).all(axis=1)]
    # df['CC1+CC2'] = df['CC1'] + df['CC2']
    df_mp = df[df['CELL_TYPE'] == 'MP']
    # df_small_jrd_mp = df_mp[df_mp['JRD_AVG'] < 19.75]
    df_pp = df[(df['CELL_TYPE'] == 'PP1') | (df['CELL_TYPE'] == 'PP2') | (df['CELL_TYPE'] == 'PP3')]
    df_ref = df[(df['CELL_TYPE'] == 'MP') | (df['CELL_TYPE'] == 'BUFFER')]
    df_pp1 = df[df['CELL_TYPE'] == 'PP1']
    df_pp2 = df[df['CELL_TYPE'] == 'PP2']
    df_pp3 = df[df['CELL_TYPE'] == 'PP3']
    df_buffer = df[df['CELL_TYPE'] == 'BUFFER']

    # Spec Shift CDF
    # print(df_pp['CC2'].mean() + 6*df_pp['CC2'].std())
    # print(df_pp1['CC2'].mean() + 6 * df_pp1['CC2'].std())
    # print(df_pp2['CC2'].mean() + 6 * df_pp2['CC2'].std())
    # print(df_pp3['CC2'].mean() + 6 * df_pp3['CC2'].std())
    # print(1 - stats.norm.cdf(1924, df_pp['CC2'].mean(), df_pp['CC2'].std()))
    # print(1 - stats.norm.cdf(1846, df_pp['CC2'].mean(), df_pp['CC2'].std()))
    # print(1 - stats.norm.cdf(1786, df_buffer['CC2'].mean(), df_buffer['CC2'].std()))
    # print(df[df['CELL_TYPE'] == 'PP1'][['CC1', 'CC2']].describe())
    # print(df[df['CELL_TYPE'] == 'PP2'][['CC1', 'CC2']].describe())
    print(df[df['CELL_TYPE'] == 'PP3'][['CC1', 'CC2']].describe())
    print(df[df['CELL_TYPE'] == 'BUFFER'][['CC1', 'CC2']].describe())
    # print(df_buffer[['CC1', 'CC2']].describe(), df_pp[['CC1', 'CC2']].describe())

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 10))
    axes = axes.flatten()
    for i, x in enumerate(['CC1', 'CC2']):
        sns.histplot(ax=axes[i], data=df, x=x, kde=True, bins=35, hue='CELL_TYPE', common_norm=False, stat='density',
                     kde_kws={'bw_adjust': 2.0},
                     hue_order=['PP3', 'BUFFER'],
                     palette=['sienna', 'dodgerblue'])
                    #bw_adjust=2.0,
        axes[i].set_xlim([df[x].min() - 120, df[x].max() + 120])
        # axes[i].axvline(x=df_ref[x].median(), color='darkblue', ls='--')  # ref median
        # axes[i].axvline(x=df_pp[x].median(), color='orange', ls='--')  # pp median
        axes[i].axvline(x=1255, color='crimson', ls='--')  # CC1 spec
        axes[i].axvline(x=1786, color='crimson', ls='--')  # CC2 spec
        axes[i].axvline(x=1910, color='black', ls='--')  # median shift
    plt.show()
    exit()


    # Correlation Matrix
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(15, 15))
    axes = axes.flatten()
    for i, df in enumerate([df_pp1, df_pp2, df_pp3, df_buffer]):
        corr_matrix = df.corr(method='pearson')
        mask = np.triu(corr_matrix)
        sns.heatmap(corr_matrix, ax=axes[i], annot=True, annot_kws={'fontsize': 8},
                    mask=mask, vmin=-0.8, vmax=0.8, cmap='RdYlBu')
        axes[i].set_title(df['CELL_TYPE'].unique())
        axes[i].set_xticklabels(labels=corr_matrix, fontsize=8, rotation=30)
        axes[i].set_yticklabels(labels=corr_matrix, fontsize=8, rotation=30)
    plt.show()
    exit()

    # Distribution Plots
    fig, axes = plt.subplots(nrows=3, ncols=4, figsize=(10, 10))
    axes = axes.flatten()
    for i, attr in enumerate(df.columns[2:]):
        sns.histplot(ax=axes[i], data=df, x=attr, hue='CELL_TYPE', bins=30, stat='density', common_norm=False, kde=True, kde_kws={'bw_adjust': 2.2},
                     hue_order=['PP1', 'PP2', 'PP3', 'BUFFER'], palette=['sienna', 'sandybrown', 'navajowhite', 'dodgerblue'])
        axes[i].set_xlim([df[attr].min() - 0.1*df[attr].std(), df[attr].max() + 0.1*df[attr].std()])
    plt.show()
    exit()

















    # JRD vs. IR
    for i, data in enumerate([df_mp, df_buffer, df_pp1, df_pp2]):
        fig = sns.jointplot(data=data, x='JRD_AVG', y='IR', height=3, ratio=2, kind='reg',
                            # xlim=[19.65, 19.95], ylim=[850, 1250],
                            # joint_kws={'levels': 5},
                            marginal_kws={'common_norm': False}
                            )
        plt.suptitle(data['CELL_TYPE'].unique())
    plt.show()
    exit()


