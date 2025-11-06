




#!/usr/bin/env python3
"""
MULTISET ANALYSIS WEB LAUNCHER
==============================
Flask web interface for launching multiset analysis modules
"""

from flask import Flask, render_template_string, jsonify, request, send_file, send_from_directory
import os
import sys
from pathlib import Path
import threading
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go


# Ensure UTF-8 encoding on Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

app = Flask(__name__)

# Global state for workflow management
workflow_status = {
    'stage': 'idle',
    'message': 'Ready to start',
    'progress': 0,
    'has_existing_data': False,
    'excel_file': None,
    'chart_configs': {}
}

# Global reference to insights analyzer
insights_analyzer = None

# HTML Template for the web interface
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Multiset Analysis System</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            padding: 20px;
        }
        .header { text-align: center; color: white; margin-bottom: 30px; }
        .header h1 { font-size: 2.5rem; text-shadow: 2px 2px 4px rgba(0,0,0,0.2); }
        .main-container { max-width: 1200px; width: 100%; }
        .menu-screen, .analysis-screen { background: white; border-radius: 15px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); padding: 40px; margin-bottom: 20px; }
        .menu-options { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-top: 30px; }
        .menu-card { background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); border-radius: 10px; padding: 30px; color: white; cursor: pointer; transition: transform 0.3s; text-align: center; }
        .menu-card:hover { transform: translateY(-5px); box-shadow: 0 15px 30px rgba(0,0,0,0.2); }
        .menu-card.insights { background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); }
        .menu-card h3 { font-size: 1.5rem; margin-bottom: 10px; }
        .status-bar { background: #f8f9fa; border-radius: 10px; padding: 20px; margin-bottom: 20px; border: 2px solid #e9ecef; }
        .progress-bar { background: #e9ecef; border-radius: 10px; height: 30px; overflow: hidden; margin: 15px 0; }
        .progress-fill { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); height: 100%; transition: width 0.3s; text-align: center; color: white; line-height: 30px; }
        .btn { padding: 12px 30px; border: none; border-radius: 5px; font-size: 1rem; cursor: pointer; margin: 5px; transition: all 0.3s; text-decoration: none; display: inline-block; }
        .btn-primary { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }
        .btn-secondary { background: #6c757d; color: white; }
        .btn-success { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color: white; }
        .btn:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(0,0,0,0.2); }
        .hidden { display: none; }
        .alert { padding: 15px; border-radius: 5px; margin: 15px 0; }
        .alert-info { background: #e3f2fd; color: #1976d2; border: 1px solid #90caf9; }
        .alert-success { background: #e8f5e9; color: #2e7d32; border: 1px solid #81c784; }
        .alert-error { background: #ffebee; color: #c62828; border: 1px solid #ef5350; }
        .control-panel { background: #f8f9fa; border-radius: 10px; padding: 25px; margin: 20px 0; }
        .control-section { margin: 20px 0; padding: 15px; background: white; border-radius: 8px; }
        .control-section h4 { color: #667eea; margin-bottom: 15px; }
        .radio-group { display: flex; flex-wrap: wrap; gap: 15px; }
        .radio-option { display: flex; align-items: center; gap: 8px; padding: 8px 15px; background: #f8f9fa; border-radius: 5px; cursor: pointer; }
        .radio-option:hover { background: #e9ecef; }
        .filter-section { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-top: 10px; }
        .filter-item label { display: block; margin-bottom: 5px; color: #666; font-weight: 500; }
        .filter-item input, .filter-item select { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 5px; }
        .chart-container { background: white; border-radius: 10px; padding: 20px; margin: 20px 0; min-height: 500px; }
        .chart-tabs { display: flex; gap: 10px; margin: 20px 0; }
        .chart-tab { padding: 10px 20px; background: #e9ecef; border-radius: 5px; cursor: pointer; transition: all 0.3s; }
        .chart-tab:hover { background: #dee2e6; }
        .chart-tab.active { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }
        .download-section { display: flex; gap: 15px; justify-content: center; margin: 30px 0; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🔬 Multiset Analysis System</h1>
        <p>Advanced Compliance and Business Intelligence</p>
    </div>
    
    <div class="main-container">
        <!-- Menu -->
        <div id="menuScreen" class="menu-screen">
            <h2>Select Analysis Type</h2>
            <div class="menu-options">
                <div class="menu-card" onclick="selectAnalysis('compliance')">
                    <h3>📊 Compliance Analysis</h3>
                    <p>Comprehensive multiset analysis for compliance monitoring</p>
                </div>
                <div class="menu-card insights" onclick="selectAnalysis('insights')">
                    <h3>💼 Business Insights</h3>
                    <p>Interactive analysis with custom filters</p>
                </div>
            </div>
            <div id="dataStatus" class="alert alert-info hidden" style="margin-top: 30px;">
                <strong>Existing data found!</strong> You can use previously parsed datasets.
            </div>
        </div>
        
        <!-- Compliance Analysis Screen -->
        <div id="complianceScreen" class="analysis-screen hidden">
            <h2>📊 Compliance Analysis</h2>
            
            <!-- Dataset Management -->
            <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                <h4 style="color: #667eea; margin-bottom: 15px;">📁 Dataset Management</h4>
                <div style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap;">
                    <button class="btn btn-primary" onclick="startCompliance(false)">🆕 Parse New CSV Files</button>
                    <button id="useExistingBtn" class="btn btn-secondary hidden" onclick="startCompliance(true)">📂 Use Existing Data</button>
                    <button class="btn btn-secondary" onclick="confirmDeleteDatasets()">🗑️ Clear All Datasets</button>
                    <button class="btn btn-secondary" onclick="backToMenu()">↩️ Back to Menu</button>
                    <span id="datasetInfo" style="margin-left: 10px; color: #666; font-size: 0.9em;"></span>
                </div>
            </div>
            
            <div class="status-bar">
                <h3>Analysis Status</h3>
                <p id="statusMessage">Ready to start</p>
                <div class="progress-bar">
                    <div id="progressFill" class="progress-fill" style="width: 0%">0%</div>
                </div>
            </div>
            
            <div id="chartSection" class="hidden">
                <div class="chart-tabs">
                    <div class="chart-tab active" onclick="selectComplianceChart('dest_count')">Destinations by Count</div>
                    <div class="chart-tab" onclick="selectComplianceChart('dest_amount')">Destinations by Amount</div>
                    <div class="chart-tab" onclick="selectComplianceChart('dest_mean')">Mean Destination Amount</div>
                    <div class="chart-tab" onclick="selectComplianceChart('origin_count')">Origins by Count</div>
                    <div class="chart-tab" onclick="selectComplianceChart('origin_amount')">Origins by Amount</div>
                    <div class="chart-tab" onclick="selectComplianceChart('origin_mean')">Mean Origin Amount</div>
                </div>
                <div style="margin: 20px 0;">
                    <label style="font-weight: 500; margin-right: 10px;">Show Top:</label>
                    <input type="range" id="topNSlider" min="5" max="50" step="5" value="20" 
                           oninput="updateTopN(this.value)" 
                           style="width: 300px; vertical-align: middle;">
                    <span id="topNValue" style="margin-left: 10px; font-weight: bold;">20</span>
                </div>
                <div class="chart-container" id="complianceChartContainer">
                    <div class="alert alert-info">Click a tab above to view charts</div>
                </div>
                <div class="download-section">
                    <a id="downloadExcel" href="#" class="btn btn-success hidden">📥 Download Excel</a>
                    <button class="btn btn-primary" onclick="window.open('/view_charts', '_blank')">🔍 View All Charts</button>
                </div>
            </div>
        </div>
        
        <!-- Business Insights Screen -->
        <div id="insightsScreen" class="analysis-screen hidden">
            <h2>🎯 Interactive Business Insights</h2>
            
            <!-- Dataset Management -->
            <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                <h4 style="color: #667eea; margin-bottom: 15px;">📁 Dataset Management</h4>
                <div style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap;">
                    <button class="btn btn-primary" onclick="initInsights()">🔄 Reload Data</button>
                    <button class="btn btn-secondary" onclick="confirmDeleteDatasets()">🗑️ Clear All Datasets</button>
                    <button class="btn btn-secondary" onclick="backToMenu()">↩️ Back to Menu</button>
                    <span id="insightsDatasetInfo" style="margin-left: 10px; color: #666; font-size: 0.9em;"></span>
                </div>
            </div>
            
            <div id="insightsInit"></div>
            
            <div class="control-panel">
                <div class="control-section">
                    <h4>1. Dataset</h4>
                    <div class="radio-group">
                        <label class="radio-option"><input type="radio" name="dataset" value="exits" checked><span>Exits</span></label>
                        <label class="radio-option"><input type="radio" name="dataset" value="inputs"><span>Inputs</span></label>
                        <label class="radio-option"><input type="radio" name="dataset" value="combined"><span>Combined</span></label>
                    </div>
                </div>
                <div class="control-section">
                    <h4>2. Group By</h4>
                    <div class="radio-group">
                        <label class="radio-option"><input type="radio" name="groupBy" value="operator" checked><span>Operator</span></label>
                        <label class="radio-option"><input type="radio" name="groupBy" value="agency"><span>Agency</span></label>
                        <label class="radio-option"><input type="radio" name="groupBy" value="destination"><span>Destination</span></label>
                        <label class="radio-option"><input type="radio" name="groupBy" value="users"><span>Users</span></label>
                    </div>
                </div>
                <div class="control-section">
                    <h4>3. Measure By</h4>
                    <div class="radio-group">
                        <label class="radio-option"><input type="radio" name="measureBy" value="amount" checked><span>Total Amount</span></label>
                        <label class="radio-option"><input type="radio" name="measureBy" value="fee"><span>Total Fee</span></label>
                        <label class="radio-option"><input type="radio" name="measureBy" value="count"><span>Transaction Count</span></label>
                        <label class="radio-option"><input type="radio" name="measureBy" value="destinations"><span>Unique Destinations</span></label>
                        <label class="radio-option"><input type="radio" name="measureBy" value="mean_amount"><span>Mean Amount per Transaction</span></label>
                        <label class="radio-option"><input type="radio" name="measureBy" value="mean_fee"><span>Mean Fee per Transaction</span></label>
                    </div>
                </div>
                <div class="control-section">
                    <h4>4. Filters (Optional)</h4>
                    <div class="filter-section">
                        <div class="filter-item"><label>Date From:</label><input type="date" id="dateFrom"></div>
                        <div class="filter-item"><label>Date To:</label><input type="date" id="dateTo"></div>
                        <div class="filter-item">
                            <label>Hour Period:</label>
                            <select id="hourPeriod">
                                <option value="">All Hours</option>
                                <option value="Morning (0-11h)">Morning (0-11h)</option>
                                <option value="Noon (12-14h)">Noon (12-14h)</option>
                                <option value="Afternoon (15-17h)">Afternoon (15-17h)</option>
                                <option value="Evening (18-23h)">Evening (18-23h)</option>
                            </select>
                        </div>
                    </div>
                    <div style="margin-top: 15px;">
                        <label style="font-weight: 500;">Show Top:</label>
                        <input type="range" id="insightsTopN" min="10" max="100" step="10" value="20" 
                               oninput="document.getElementById('insightsTopNValue').textContent = this.value"
                               style="width: 200px; margin: 0 10px;">
                        <span id="insightsTopNValue" style="font-weight: bold;">20</span>
                    </div>
                </div>
                <div style="text-align: center; margin-top: 30px;">
                    <button id="generateInsights" class="btn btn-primary" onclick="runInsights()">🚀 Generate Analysis</button>
                </div>
            </div>
            
            <div id="insightsResults" class="hidden">
                <h3>📊 Analysis Results</h3>
                <div id="insightsSummary" style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 15px 0;"></div>
                <div class="chart-container" id="insightsChartContainer"></div>
            </div>
        </div>
    </div>
    
    <script>
        // Make functions globally accessible immediately
        let currentComplianceChart = 'dest_count';
        let complianceChartData = {};
        let statusInterval;
        let currentTopN = 20;
        
        // Define all functions globally from the start
        window.selectAnalysis = function(type) {
            console.log('selectAnalysis called with:', type);
            try {
                const menuScreen = document.getElementById('menuScreen');
                const complianceScreen = document.getElementById('complianceScreen');
                const insightsScreen = document.getElementById('insightsScreen');
                
                console.log('Elements found:', {
                    menuScreen: !!menuScreen,
                    complianceScreen: !!complianceScreen,
                    insightsScreen: !!insightsScreen
                });
                
                menuScreen.classList.add('hidden');
                
                if (type === 'compliance') {
                    complianceScreen.classList.remove('hidden');
                    checkStatus();
                } else if (type === 'insights') {
                    insightsScreen.classList.remove('hidden');
                    initInsights();
                }
            } catch (err) {
                console.error('selectAnalysis error:', err);
                alert('Error switching screens: ' + err.message);
            }
        };
        
        window.backToMenu = function() {
            document.querySelectorAll('.analysis-screen').forEach(s => s.classList.add('hidden'));
            document.getElementById('menuScreen').classList.remove('hidden');
            if (statusInterval) clearInterval(statusInterval);
        };
        
        window.checkStatus = function() {
            fetch('/api/status')
                .then(r => r.json())
                .then(data => {
                    if (data.has_existing_data) {
                        document.getElementById('dataStatus').classList.remove('hidden');
                        document.getElementById('useExistingBtn').classList.remove('hidden');
                        document.getElementById('datasetInfo').textContent = '✓ Existing datasets available';
                        document.getElementById('datasetInfo').style.color = '#28a745';
                    } else {
                        document.getElementById('datasetInfo').textContent = '⚠ No datasets found - please parse CSV files';
                        document.getElementById('datasetInfo').style.color = '#dc3545';
                    }
                });
        };
        
        window.confirmDeleteDatasets = function() {
            if (confirm('⚠️ This will delete ALL parsed datasets. Are you sure?\n\nYou will need to parse CSV files again.')) {
                fetch('/api/delete_datasets', {method: 'POST'})
                    .then(r => r.json())
                    .then(data => {
                        if (data.success) {
                            alert('✓ All datasets have been deleted successfully');
                            location.reload();
                        } else {
                            alert('❌ Error: ' + data.error);
                        }
                    })
                    .catch(err => alert('❌ Error: ' + err.message));
            }
        };
        
        window.startCompliance = function(useExisting = false) {
            fetch('/api/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({use_existing: useExisting})
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    statusInterval = setInterval(updateComplianceStatus, 1000);
                    document.getElementById('useExistingBtn').disabled = true;
                }
            });
        };
        
        window.updateComplianceStatus = function() {
            fetch('/api/status')
                .then(r => r.json())
                .then(data => {
                    document.getElementById('statusMessage').textContent = data.message;
                    document.getElementById('progressFill').style.width = data.progress + '%';
                    document.getElementById('progressFill').textContent = data.progress + '%';
                    
                    if (data.stage === 'complete') {
                        clearInterval(statusInterval);
                        loadComplianceCharts();
                        if (data.excel_file) {
                            const downloadBtn = document.getElementById('downloadExcel');
                            downloadBtn.href = '/download/' + data.excel_file;
                            downloadBtn.classList.remove('hidden');
                        }
                        document.getElementById('chartSection').classList.remove('hidden');
                    } else if (data.stage === 'error') {
                        clearInterval(statusInterval);
                        document.getElementById('useExistingBtn').disabled = false;
                    }
                });
        };
        
        window.loadComplianceCharts = function() {
            document.getElementById('complianceChartContainer').innerHTML = 
                '<div class="alert alert-info">Loading charts...</div>';
            
            fetch('/api/chart_data')
                .then(r => r.json())
                .then(data => {
                    complianceChartData = data;
                    if (Object.keys(data).length > 0) {
                        renderComplianceChart();
                    } else {
                        document.getElementById('complianceChartContainer').innerHTML = 
                            '<div class="alert alert-error">❌ No chart data available</div>';
                    }
                })
                .catch(err => {
                    document.getElementById('complianceChartContainer').innerHTML = 
                        '<div class="alert alert-error">❌ Error: ' + err.message + '</div>';
                });
        };
        
        window.selectComplianceChart = function(chartType) {
            currentComplianceChart = chartType;
            document.querySelectorAll('.chart-tab').forEach(tab => tab.classList.remove('active'));
            event.target.classList.add('active');
            
            // If data not loaded yet, show message
            if (Object.keys(complianceChartData).length === 0) {
                document.getElementById('complianceChartContainer').innerHTML = 
                    '<div class="alert alert-info">⏳ Please wait for analysis to complete...</div>';
                return;
            }
            
            renderComplianceChart();
        };
        
        window.updateTopN = function(value) {
            currentTopN = parseInt(value);
            document.getElementById('topNValue').textContent = value;
            renderComplianceChart();
        };
        
        window.renderComplianceChart = function() {
            if (!complianceChartData[currentComplianceChart]) {
                document.getElementById('complianceChartContainer').innerHTML = 
                    '<div class="alert alert-info">Chart not available</div>';
                return;
            }
            
            const chartConfig = complianceChartData[currentComplianceChart];
            const data = chartConfig.data.slice(0, currentTopN);
            
            if (data.length === 0) {
                document.getElementById('complianceChartContainer').innerHTML = 
                    '<div class="alert alert-info">No data available</div>';
                return;
            }
            
            let xValues, yValues, yLabel, xLabel;
            
            // Map chart types to their specific data fields
            if (currentComplianceChart === 'dest_count') {
                xValues = data.map(d => d.destination);
                yValues = data.map(d => d.count);
                yLabel = 'Transaction Count';
                xLabel = 'Destination';
            } else if (currentComplianceChart === 'dest_amount') {
                xValues = data.map(d => d.destination);
                yValues = data.map(d => d.total);
                yLabel = 'Total Amount';
                xLabel = 'Destination';
            } else if (currentComplianceChart === 'dest_mean') {
                xValues = data.map(d => d.destination);
                yValues = data.map(d => d.mean_amount);
                yLabel = 'Mean Amount';
                xLabel = 'Destination';
            } else if (currentComplianceChart === 'origin_count') {
                xValues = data.map(d => d.origin);
                yValues = data.map(d => d.count);
                yLabel = 'Transaction Count';
                xLabel = 'Origin';
            } else if (currentComplianceChart === 'origin_amount') {
                xValues = data.map(d => d.origin);
                yValues = data.map(d => d.total);
                yLabel = 'Total Amount';
                xLabel = 'Origin';
            } else if (currentComplianceChart === 'origin_mean') {
                xValues = data.map(d => d.origin);
                yValues = data.map(d => d.mean_amount);
                yLabel = 'Mean Amount';
                xLabel = 'Origin';
            }
            
            // Generate colors
            const colors = generateColors(data.length);
            
            const trace = {
                x: xValues,
                y: yValues,
                type: 'bar',
                marker: {color: colors},
                text: yValues.map(v => v > 100 ? v.toFixed(0) : v.toFixed(2)),
                textposition: 'auto'
            };
            
            const layout = {
                title: chartConfig.title + ' (Top ' + currentTopN + ')',
                xaxis: {
                    title: xLabel,
                    tickangle: -45
                },
                yaxis: {
                    title: yLabel
                },
                margin: {b: 150},
                template: 'plotly_white',
                height: 600
            };
            
            Plotly.newPlot('complianceChartContainer', [trace], layout, {responsive: true});
        };
        
        window.generateColors = function(count) {
            const colors = [
                '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
                '#74B9FF', '#A29BFE', '#FD79A8', '#FDCB6E', '#6C5CE7',
                '#00B894', '#00CEC9', '#0984E3', '#FDCB6E', '#E17055'
            ];
            
            const result = [];
            for (let i = 0; i < count; i++) {
                result.push(colors[i % colors.length]);
            }
            return result;
        };
        
        window.initInsights = function() {
            document.getElementById('insightsInit').innerHTML = '<div class="alert alert-info">Initializing...</div>';
            fetch('/api/insights/init', {method: 'POST'})
                .then(r => r.json())
                .then(data => {
                    if (data.success) {
                        document.getElementById('insightsInit').innerHTML = 
                            `<div class="alert alert-success">✅ Ready! ${data.info}</div>`;
                        document.getElementById('insightsDatasetInfo').textContent = '✓ ' + data.info;
                        document.getElementById('insightsDatasetInfo').style.color = '#28a745';
                        document.getElementById('generateInsights').disabled = false;
                    } else {
                        document.getElementById('insightsInit').innerHTML = 
                            `<div class="alert alert-error">❌ ${data.error}</div>`;
                        document.getElementById('insightsDatasetInfo').textContent = '⚠ ' + data.error;
                        document.getElementById('insightsDatasetInfo').style.color = '#dc3545';
                        document.getElementById('generateInsights').disabled = true;
                    }
                });
        };
        
        window.runInsights = function() {
            const topN = parseInt(document.getElementById('insightsTopN').value);
            const params = {
                dataset: document.querySelector('input[name="dataset"]:checked').value,
                group_by: document.querySelector('input[name="groupBy"]:checked').value,
                measure_by: document.querySelector('input[name="measureBy"]:checked').value,
                top_n: topN,
                filters: {
                    date_from: document.getElementById('dateFrom').value,
                    date_to: document.getElementById('dateTo').value,
                    hour_period: document.getElementById('hourPeriod').value
                }
            };
            
            document.getElementById('insightsResults').classList.remove('hidden');
            document.getElementById('insightsChartContainer').innerHTML = '<div class="alert alert-info">Generating analysis...</div>';
            
            fetch('/api/insights/analyze', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(params)
            })
            .then(r => r.json())
            .then(data => {
                if (data.success) {
                    // Show summary
                    let summaryHTML = `<strong>Summary:</strong> Found ${data.data_count} total records. Showing top ${topN}.`;
                    if (data.total_transactions) {
                        summaryHTML += ` | Total Transactions: ${data.total_transactions.toLocaleString()}`;
                    }
                    if (data.mean_value) {
                        summaryHTML += ` | Overall Mean: ${data.mean_value.toFixed(2)}`;
                    }
                    document.getElementById('insightsSummary').innerHTML = summaryHTML;
                    
                    // Render chart
                    const fig = JSON.parse(data.figure_json);
                    Plotly.newPlot('insightsChartContainer', fig.data, fig.layout, {
                        responsive: true,
                        displayModeBar: true,
                        modeBarButtonsToRemove: ['lasso2d', 'select2d'],
                        displaylogo: false
                    });
                } else {
                    document.getElementById('insightsChartContainer').innerHTML = 
                        `<div class="alert alert-error">❌ ${data.error}</div>`;
                }
            })
            .catch(err => {
                document.getElementById('insightsChartContainer').innerHTML = 
                    `<div class="alert alert-error">❌ Error: ${err.message}</div>`;
            });
        };
        
        // Initialize app when DOM is ready
        function initApp() {
            console.log('App initialized - DOM ready');
            
            // Test menu cards are clickable
            const cards = document.querySelectorAll('.menu-card');
            console.log('Found', cards.length, 'menu cards');
            
            // Check initial status on load
            fetch('/api/status')
                .then(r => r.json())
                .then(data => {
                    console.log('Status:', data);
                    if (data.has_existing_data) {
                        document.getElementById('dataStatus').classList.remove('hidden');
                    }
                })
                .catch(err => console.error('Status check failed:', err));
        }
        
        // Error handler
        window.onerror = function(msg, url, line, col, error) {
            console.error('JavaScript Error:', msg, 'Line:', line);
            return false;
        };
        
        // Ensure DOM is loaded
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', initApp);
        } else {
            initApp();
        }
    </script>
</body>
</html>
"""


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def check_existing_data():
    """Check if parsed datasets exist"""
    storage_dir = Path("parsed_datasets")
    if storage_dir.exists():
        sessions = sorted([d for d in storage_dir.iterdir() if d.is_dir()])
        return len(sessions) > 0
    return False


def run_compliance_analysis(use_existing=False):
    """Run compliance workflow by calling external modules"""
    global workflow_status
    
    try:
        if not use_existing:
            workflow_status['stage'] = 'parsing'
            workflow_status['message'] = 'Parsing CSV files...'
            workflow_status['progress'] = 30
            
            # Import and run parser
            try:
                from interactive_csv_parser_system import main as parser_main
                data_manager = parser_main()
                
                if not data_manager:
                    workflow_status['stage'] = 'error'
                    workflow_status['message'] = 'Parsing cancelled or failed'
                    return
            except ImportError as e:
                workflow_status['stage'] = 'error'
                workflow_status['message'] = f'Parser module not found: {e}'
                return
            except Exception as e:
                workflow_status['stage'] = 'error'
                workflow_status['message'] = f'Parser error: {e}'
                return
        
        workflow_status['stage'] = 'analyzing'
        workflow_status['message'] = 'Running compliance analysis...'
        workflow_status['progress'] = 70
        
        # Import and run analyzer
        try:
            from multiset_analyzer import main as analyzer_main
            excel_file, chart_configs = analyzer_main()
            
            if excel_file:
                workflow_status['stage'] = 'complete'
                workflow_status['message'] = 'Analysis complete!'
                workflow_status['progress'] = 100
                # Store only filename, not full path
                workflow_status['excel_file'] = Path(excel_file).name
                workflow_status['chart_configs'] = chart_configs
            else:
                workflow_status['stage'] = 'error'
                workflow_status['message'] = 'Analysis returned no results'
                workflow_status['progress'] = 0
        except ImportError as e:
            workflow_status['stage'] = 'error'
            workflow_status['message'] = f'Analyzer module not found: {e}'
        except Exception as e:
            workflow_status['stage'] = 'error'
            workflow_status['message'] = f'Analysis error: {e}'
        
    except Exception as e:
        workflow_status['stage'] = 'error'
        workflow_status['message'] = f'Unexpected error: {e}'
        workflow_status['progress'] = 0


# ============================================================================
# FLASK ROUTES
# ============================================================================

@app.route('/')
def index():
    """Serve the main web interface"""
    workflow_status['has_existing_data'] = check_existing_data()
    return render_template_string(HTML_TEMPLATE)


@app.route('/api/status')
def get_status():
    """Return current workflow status"""
    workflow_status['has_existing_data'] = check_existing_data()
    return jsonify(workflow_status)


@app.route('/api/start', methods=['POST'])
def start_workflow():
    """Start the compliance workflow in a background thread"""
    data = request.json or {}
    use_existing = data.get('use_existing', False)
    
    if workflow_status['stage'] not in ['idle', 'complete', 'error']:
        return jsonify({'success': False, 'error': 'Workflow already in progress'})
    
    workflow_status['stage'] = 'starting'
    workflow_status['message'] = 'Initializing workflow...'
    workflow_status['progress'] = 10
    
    thread = threading.Thread(target=run_compliance_analysis, args=(use_existing,))
    thread.start()
    
    return jsonify({'success': True})


@app.route('/api/chart_data')
def get_chart_data():
    """Return chart configuration data"""
    return jsonify(workflow_status.get('chart_configs', {}))


@app.route('/download/<filename>')
def download_file(filename):
    """Download the Excel file"""
    try:
        # Look for the file in the current directory
        return send_file(filename, as_attachment=True)
    except Exception as e:
        return f"File not found: {e}", 404


@app.route('/view_charts')
def view_charts():
    """View all charts in a separate page"""
    charts_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>All Charts - Multiset Analysis</title>
        <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 20px;
            }
            .container {
                max-width: 1400px;
                margin: 0 auto;
                background: white;
                border-radius: 15px;
                padding: 30px;
            }
            .chart-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
                gap: 30px;
                margin-top: 20px;
            }
            .chart-item {
                background: #f8f9fa;
                border-radius: 10px;
                padding: 20px;
            }
            h1 { 
                color: #667eea; 
                text-align: center;
                margin-bottom: 30px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 All Analysis Charts</h1>
            <div class="chart-grid" id="chartsContainer"></div>
        </div>
        <script>
            fetch('/api/chart_data')
                .then(r => r.json())
                .then(data => {
                    const container = document.getElementById('chartsContainer');
                    const chartTypes = Object.keys(data);
                    
                    if (chartTypes.length === 0) {
                        container.innerHTML = '<p>No charts available. Please run the analysis first.</p>';
                        return;
                    }
                    
                    chartTypes.forEach((type, index) => {
                        const config = data[type];
                        const chartDiv = document.createElement('div');
                        chartDiv.className = 'chart-item';
                        
                        const plotDiv = document.createElement('div');
                        plotDiv.id = 'chart' + index;
                        chartDiv.appendChild(plotDiv);
                        container.appendChild(chartDiv);
                        
                        // Create trace
                        let xValues, yValues;
                        const chartData = config.data.slice(0, 20);
                        
                        if (type.includes('dest')) {
                            xValues = chartData.map(d => d.destination);
                            if (type.includes('count')) yValues = chartData.map(d => d.count);
                            else if (type.includes('amount')) yValues = chartData.map(d => d.total);
                            else if (type.includes('mean')) yValues = chartData.map(d => d.mean_amount);
                        } else {
                            xValues = chartData.map(d => d.origin);
                            if (type.includes('count')) yValues = chartData.map(d => d.count);
                            else if (type.includes('amount')) yValues = chartData.map(d => d.total);
                            else if (type.includes('mean')) yValues = chartData.map(d => d.mean_amount);
                        }
                        
                        const trace = {
                            x: xValues,
                            y: yValues,
                            type: 'bar',
                            marker: {
                                color: '#667eea'
                            }
                        };
                        
                        const layout = {
                            title: config.title,
                            xaxis: {tickangle: -45},
                            margin: {b: 150},
                            height: 400
                        };
                        
                        Plotly.newPlot(plotDiv.id, [trace], layout);
                    });
                })
                .catch(err => {
                    document.getElementById('chartsContainer').innerHTML = 
                        '<p>Error loading charts: ' + err.message + '</p>';
                });
        </script>
    </body>
    </html>
    """
    return charts_html


@app.route('/api/insights/init', methods=['POST'])
def init_insights():
    """Initialize the insights analyzer"""
    global insights_analyzer
    
    try:
        # Import insights module
        from multiset_insights import MultisetInsights
        
        # Initialize analyzer
        insights_analyzer = MultisetInsights()
        
        # Load datasets
        if insights_analyzer.load_datasets():
            # Count datasets
            dataset_count = len(insights_analyzer.datasets)
            
            return jsonify({
                'success': True,
                'info': f'Loaded {dataset_count} datasets successfully'
            })
        else:
            return jsonify({
                'success': False, 
                'error': 'No datasets found. Please run the CSV parser first.'
            })

    except ImportError as e:
        return jsonify({
            'success': False, 
            'error': f'Could not import insights module: {e}'
        })
    except Exception as e:
        import traceback
        return jsonify({
            'success': False, 
            'error': str(e), 
            'trace': traceback.format_exc()
        })


@app.route('/api/delete_datasets', methods=['POST'])
def delete_datasets():
    """Delete all parsed datasets"""
    try:
        import shutil
        parsed_dir = Path('parsed_datasets')
        if parsed_dir.exists():
            shutil.rmtree(parsed_dir)
            return jsonify({'success': True, 'message': 'All datasets deleted'})
        else:
            return jsonify({'success': True, 'message': 'No datasets to delete'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/insights/analyze', methods=['POST'])
def analyze_insights():
    """Run insights analysis with user parameters"""
    global insights_analyzer
    
    if insights_analyzer is None:
        return jsonify({
            'success': False, 
            'error': 'Insights analyzer not initialized. Please initialize first.'
        })
    
    try:
        params = request.json
        top_n = params.get('top_n', 20)
        
        # Build filters
        filters = {}
        if params['filters'].get('date_from'):
            filters['date_from'] = params['filters']['date_from']
        if params['filters'].get('date_to'):
            filters['date_to'] = params['filters']['date_to']
        if params['filters'].get('hour_period'):
            filters['hour_period'] = params['filters']['hour_period']
        
        # Handle mean calculations
        measure_by = params['measure_by']
        if measure_by in ['mean_amount', 'mean_fee']:
            # Convert to base measure for analysis
            base_measure = measure_by.replace('mean_', '')
            result = insights_analyzer.analyze_dynamic(
                params['dataset'],
                params['group_by'],
                'count',  # Get count for mean calculation
                filters if filters else None
            )
            
            if result is None or result.empty:
                return jsonify({'success': False, 'error': 'No data returned'})
            
            # Get totals
            result_totals = insights_analyzer.analyze_dynamic(
                params['dataset'],
                params['group_by'],
                base_measure,
                filters if filters else None
            )
            
            # Calculate mean
            result['mean'] = result_totals.iloc[:, 1] / result.iloc[:, 1]
            result = result.sort_values('mean', ascending=False).reset_index(drop=True)
            y_col = 'mean'
            y_label = f'Mean {base_measure.title()} per Transaction'
        else:
            # Run normal analysis
            result = insights_analyzer.analyze_dynamic(
                params['dataset'],
                params['group_by'],
                measure_by,
                filters if filters else None
            )
            
            if result is None or result.empty:
                return jsonify({'success': False, 'error': 'No data returned'})
            
            y_col = result.columns[1]
            y_label = y_col.replace('_', ' ').title()
        
        # Limit to top N
        result_display = result.head(top_n)
        x_col = result_display.columns[0]
        
        # Calculate summary stats
        total_count = len(result)
        total_transactions = int(result.iloc[:, 1].sum()) if measure_by == 'count' else None
        mean_value = float(result.iloc[:, 1].mean()) if len(result) > 0 else None
        
        # Create vertical bar chart with better tooltips
        colors = px.colors.qualitative.Plotly * (len(result_display) // len(px.colors.qualitative.Plotly) + 1)
        
        fig = go.Figure(data=[
            go.Bar(
                x=result_display[x_col],
                y=result_display[y_col],
                marker_color=colors[:len(result_display)],
                text=result_display[y_col].apply(lambda x: f'{x:,.2f}' if x < 1000 else f'{x:,.0f}'),
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>' + y_label + ': %{y:,.2f}<br><extra></extra>'
            )
        ])
        
        fig.update_layout(
            title=f"{params['group_by'].title()} by {measure_by.replace('_', ' ').title()} (Top {top_n})",
            xaxis_title=x_col.title(),
            yaxis_title=y_label,
            xaxis_tickangle=-45,
            height=600,
            template='plotly_white',
            showlegend=False,
            hovermode='closest'
        )
        
        return jsonify({
            'success': True,
            'figure_json': fig.to_json(),
            'data_count': total_count,
            'total_transactions': total_transactions,
            'mean_value': mean_value
        })
        
    except Exception as e:
        import traceback
        return jsonify({
            'success': False, 
            'error': str(e), 
            'trace': traceback.format_exc()
        })


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    print("\n" + "="*70)
    print("MULTISET ANALYSIS WEB LAUNCHER")
    print("="*70)
    print("\n📍 Starting web server at http://localhost:5000")
    print("💡 The browser will open automatically...\n")
    
    # Auto-open browser after short delay
    import webbrowser
    threading.Timer(1.5, lambda: webbrowser.open('http://localhost:5000')).start()
    
    # Run Flask app
    app.run(debug=False, port=5000, host='0.0.0.0')