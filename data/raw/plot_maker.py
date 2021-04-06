#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 17:58:47 2020

@author: mcgaritym
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import folium
from folium import plugins
from folium.plugins import HeatMap
import numpy as np
from datetime import date, datetime
import time
from zipfile import ZipFile
import glob
import os
import seaborn as sns
import textwrap
from mpl_toolkits.mplot3d import Axes3D
import squarify
from urllib.request import urlopen
import json


# # =============================================================================
# # 
# # =============================================================================


def plot_maker(x, y, data):
    
# =============================================================================
# Size SMALL    
# =============================================================================
 
    sizes = [(6.4, 4.8), (13,10)]
    styles = ['seaborn-whitegrid', 'seaborn-darkgrid']
    dpi= [100, 200]
    
    # size loop for small and large plots
    for size in sizes:     
        
        # style loop for white and dark grid
        for style in styles:
            
            # dpi loop for resolution
            for res in dpi:
            
                # set style and size
                plt.style.use(style)
                fig, ax = plt.subplots(figsize = size, dpi=res)
            
                
                # =============================================================================
                #  LINE PLOT           
                # =============================================================================
                
                # define x, y, and plot
                x = ..
                y = ..
                plt.plot(x,y) 
                
                # labels, limits, title
                #plt.xlim(0, max(x)*1.10)
                #plt.ylim(0, max(y)*1.10)
                plt.xticks()
                plt.yticks()
                plt.title('TITLE here')
                plt.xlabel("X-AXIS here")
                plt.ylabel("Y-AXIS here")

                # additional line HERE                
                #plt.plot(x,np.cos(x), label='y=cos(x)')
                #plt.legend()
                
                plt.show()  
                plt.save_fig('line_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')
                
                # =============================================================================
                #  SCATTER PLOT           
                # =============================================================================
                
                x = ..
                y = ..
                plt.scatter(x,y) 
                
                # labels, limits, title
                #plt.xlim(0, max(x)*1.10)
                #plt.ylim(0, max(y)*1.10)
                plt.xticks()
                plt.yticks()
                plt.title('TITLE here')
                plt.xlabel("X-AXIS here")
                plt.ylabel("Y-AXIS here")

                # additional line HERE                
                #plt.plot(x,np.cos(x), label='y=cos(x)')
                #plt.legend()
                
                plt.show()  
                plt.save_fig('scatter_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               
                

                # =============================================================================
                #  3-D SCATTER PLOT           
                # =============================================================================
                
                fig = plt.figure(figsize=size)
                ax = fig.add_subplot(111, projection='3d')
                ax.scatter(x, y, z, s=30)
                ax.set(xlabel='X-AXIS here', ylabel='Y-AXIS here',zlabel='Z-AXIS here')
                #plt.xticks(np.arange(2015,2020,step =1))
                plt.title('TITLE here')
                plt.show()
                
                # x = ..
                # y = ..
                # plt.scatter(x,y) 
                
                # # labels, limits, title
                # #plt.xlim(0, max(x)*1.10)
                # #plt.ylim(0, max(y)*1.10)
                # plt.title('TITLE here')
                # plt.xlabel("X-AXIS here")
                # plt.ylabel("Y-AXIS here")

                # # additional line HERE                
                # #plt.plot(x,np.cos(x), label='y=cos(x)')
                # #plt.legend()
                
                plt.show()  
                plt.save_fig('scatter_3D_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               
                
                # =============================================================================
                #  REGRESSION PLOT           
                # =============================================================================
                
                df_fit = np.polyfit(x, y, 1)

                # Scatter plots.
                plt.scatter(x, y)
                
                # regression lines
                plt.plot(df.x, df_fit[0] * df.x + df_fit[1], color='darkblue', linewidth=2)
                
                # regression equations
                plt.text(65, 230, 'y={:.2f}+{:.2f}*x'.format(df_fit[1], df_fit[0]), color='darkblue', size=12)
                
                # legend, title and labels.
                plt.title('Relationship between Height and Weight', size=24)
                plt.xlabel('Height (inches)', size=18)
                plt.ylabel('Weight (pounds)', size=18);
                
                plt.show()
                plt.save_fig('scatter_linear_reg_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               

                
                # =============================================================================
                #  AREA PLOT           
                # =============================================================================
            
                # Regular Area Chart
                # Create data
                x = ...
                y = ...
                # Change the color and its transparency
                plt.fill_between( x, y, color="skyblue", alpha=0.2)
                plt.xlabel('Score')
                plt.ylabel('GDP')
                plt.plot(x, y, color="Slateblue", alpha=0.6)
                
                plt.show()
                plt.save_fig('area_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               

                # =============================================================================
                #  STACKED AREA PLOT           
                # =============================================================================
                    
                plt.stackplot(df_top.index,
                              [df_top['GDP'], df_top['Health'],
                               df_top['Support'], df_top['Freedom']],
                              df_top['Generosity'], df_top['Corruption'],
                              labels=['GDP', 'Health', 'Support', 'Freedom','Generosity','Corruption'],
                              alpha=0.8)
                plt.legend(loc=2, fontsize='large')
                
                plt.show()
                plt.save_fig('stacked_area_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               

                # =============================================================================
                #  100% STACKED AREA PLOT           
                # =============================================================================
                    
                data_perc = df_top[['GDP', 'Health', 'Support', 'Freedom','Generosity','Corruption']]
                data_perc = data_perc.divide(data_perc.sum(axis=1), axis=0)
                plt.stackplot(data_perc.index,
                  [data_perc['GDP'], data_perc['Health'],
                   data_perc['Support'], data_perc['Freedom']],
                  data_perc['Generosity'], data_perc['Corruption'],
                  labels=['GDP', 'Health', 'Support', 'Freedom','Generosity','Corruption'],
                  alpha=0.8)
                plt.legend(loc=2, fontsize='large')
                 
                plt.show()
                plt.save_fig('100%stacked_area_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               
                
                # =============================================================================
                #  BAR (HORIZONTAL) PLOT           
                # =============================================================================
                
                countries = ['United States','Japan', 'Germany','Brazil', 'India']
                y_pos = np.arange(len(countries))
                data = df_19[(df_19['Country'].isin(countries))].sort_values(['Country'])
                data.sort_values('GDP', inplace=True)
                data.reset_index(drop=True)
                plt.bar(y_pos, data['GDP'], align='center', alpha=0.5)
                plt.xticks(y_pos, data['Country'])
                plt.ylabel('GDP')
                plt.title('Bar (Vertical) Plot')
                
                plt.show()
                plt.save_fig('bar_horizontal_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               


                # =============================================================================
                #  BAR (VERTICAL) PLOT           
                # =============================================================================
                 
                df_19[(df_19['Country'].isin(countries))].sort_values(['Country'])
                data.sort_values('Score', inplace=True)
                data.reset_index(drop=True)
                plt.barh(y_pos, data['Score'], align='center', alpha=0.5)
                plt.yticks(y_pos, data['Country'])
                plt.xlabel('Score')
                plt.title('Bar (Horizontal) Chart')
                
                plt.show()
                plt.save_fig('bar_vertical_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               

                # =============================================================================
                #  BAR (GROUPED) PLOT           
                # =============================================================================

                # Group bar chart
                index = np.arange(5)
                width = 0.35
                fig, ax = plt.subplots(figsize=(9, 6))
                rects1 = ax.bar(index - width / 2, data['GDP'],
                                width, color='#1f77b4', alpha=0.5)
                rects2 = ax.bar(index + width / 2, data['Health'],
                                width, color='#1f77b4')
                plt.xticks(index, gdp['Country'])
                plt.legend((rects1[0], rects2[0]), ('GDP', 'Health'))
                
                plt.show()
                plt.save_fig('bar_grouped_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               
                
                # =============================================================================
                #  BAR (STACKED) PLOT           
                # =============================================================================


                # Stacked bar chart
                fig = plt.figure(figsize=(14,10))
                rect1 = plt.bar(np.arange(5), data['Support'],
                                width=0.5, color='lightblue')
                rect2 = plt.bar(np.arange(5), data['Freedom'],
                                width=0.5, color='#1f77b4')
                plt.xticks(index, data['Country'])
                plt.legend((rect1[0], rect2[0]), ('Support', 'Freedom'))
                
                plt.show()
                plt.save_fig('bar_stacked_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               


                # =============================================================================
                #  LOLLIPLOP PLOT           
                # =============================================================================
                
                # lollipip chart
                (markerline, stemlines, baseline) = plt.stem(data['Country'],
                                             data['GDP'])
                plt.setp(markerline, marker='o', markersize=15,
                         markeredgewidth=2, color='lightblue')
                plt.setp(stemlines, color='lightblue')
                plt.setp(baseline, visible=False)
                plt.tick_params(labelsize=12)
                plt.ylabel('GDP', size=12)
                plt.ylim(bottom=0)
                
                plt.show()
                plt.save_fig('lollipop_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               

                
                # =============================================================================
                #  HISTOGRAM          
                # =============================================================================
                          
                #fig = plt.figure(figsize=(8,6))
                plt.hist(df_19['Corruption'], bins=6, density=True)
                plt.grid(alpha=0.2)
                
                plt.show()
                plt.save_fig('histogram_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               

                # =============================================================================
                #  BOX (WHISKER) PLOT          
                # =============================================================================
                     
                # Create dataset
                user_1 = [10, 3, 15, 21, 17, 14]
                user_2 = [5, 13, 10, 7, 9, 12]
                data = [user_1, user_2]
                fig = plt.figure(figsize =(8, 6)) 
                  
                # Create axes instance 
                ax = fig.add_axes([0, 0, 1, 1]) 
                  
                # Create plot 
                bp = ax.boxplot(data) 
                  
                # Show plot 
                plt.xticks([1,2],['user_1','user_2'])
                
                plt.show()
                plt.save_fig('box_whisker_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               

            
                # =============================================================================
                #  PIE CHART         
                # =============================================================================
                
                # Pie Chart
                fig = plt.figure(figsize=(8,8))
                labels = 'Jan', 'Feb', 'March', 'April', 'May', 'June'
                user_1 = [10, 3, 15, 21, 17, 14]
                p = plt.pie(user_1, labels=labels, explode=(0.07, 0, 0, 0, 0, 0),
                            autopct='%1.1f%%', startangle=130, shadow=True)
                plt.axis('equal')
                for i, (Jan, Feb, March, April, May, June) in enumerate(p):
                    if i > 0:
                        Jan.set_fontsize(12)
                        Feb.set_fontsize(12)
                        March.set_fontsize(12)
                        April.set_fontsize(12)
                        May.set_fontsize(12)
                        June.set_fontsize(12)
                
                plt.show()
                plt.save_fig('pie_chart_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               

                
                # =============================================================================
                #  TREEMAP CHART         
                # =============================================================================
                
                fig = plt.figure(figsize=(10,10))
                articles = [17, 22, 35, 41, 5, 12, 47]
                labels = ['User_1:\n 17 articles',
                          'User_2:\n 22 articles',
                          'User_3:\n 35 articles',
                          'User_4:\n 41 articles',
                          'User_5:\n 5 articles',
                          'User_6:\n 12 articles',
                          'User_7:\n 47 articles']
                color_list = ['#0f7216', '#b2790c', '#ffe9a3',
                              '#f9d4d4', '#d35158', '#ea3033']
                plt.rc('font', size=14)        
                squarify.plot(sizes=articles, label=labels,
                              color=color_list, alpha=0.7)
                plt.axis('off')
                
                plt.show()
                plt.save_fig('treemap_chart_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               
                
                # =============================================================================
                #  TIME SERIES PLOT         
                # =============================================================================
                
                # Time Series Plot
                #plt.figure(figsize=(8,6))
                ts = pd.Series(np.random.randn(100), index = pd.date_range( 
                                                '1/1/2020', periods = 100)) 
                # Return the cumulative sum of the elements.
                ts = ts.cumsum() 
                ts.plot() 
                
                plt.show()
                plt.save_fig('time_series_plot_' + size[0] + 'x' + size[1] + '_' + style + '_' + res + '.png')               

                # =============================================================================
                #  GEO MAP         
                # =============================================================================
                  
    
    
    
    
    
    
    
    