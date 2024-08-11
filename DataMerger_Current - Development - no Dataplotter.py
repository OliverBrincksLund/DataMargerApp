'''
Todo:
- Complete reset to baseline when performing merge with new methos?
- Filter should be more aggresive. If i filter PURPOSE to be == V, then VV should not show up. 

Option to groupby and aggregate the data based on a selected column(s) and a value column?? (DONE)
- Testing required

Remove individual loaded data files secondary to clear memory? (DONE)

Issue when merging between two tables with different column names even though the data is the same. Issue is that the column from second dataset that is used to merge with
is also added to the data even though that is not needed since a column with that data already exists in the main dataset, just with a different name. (DONE)

Allow statistics to be calculated for either main data or the currently selected secondray data. (DONE)

Issue when adding points to mapview if there are many data points with same coordinates. (DONE)

Manual change of column names in the tables and order of columns in the dataframe? (DONE)

clear filters for secondary data. (DONE)
- Keep testing to make sure this does not break anything.

Option to load .shp files. as main data (DONE)

Fix naming of tables after operatins like filtering, clearing filters and aggregating. Just maintain the tables original name when loaded. (DONE)

Columns in tables can only be sorted by clicking on the column header for integer and float values.

-	NOTE: Da nogle datarækker ikke har DGUNR (nan) i databasen (pestDB), bliver disse fjernet under aggregering. De kan ikke bibeholdes? (DONE)
Måske det er fordi de/rådgiveren endnu ikke har fået dem indrapporteret til jupiter, men kun egen database? (DONE)

Move export of main and secondary data to toolbar menu (DONE)

Move advancedanalysis to a tab of its own instead of popup window (DONE)

Fix FilterDialog single filter removal. Currently, it does not correctly reflect in the data when a single filter is removed, only when all are cleared.

Fix the edit columns. Very slow and also renaming columns wipes the data in that column?
'''
# Standard library imports
import csv
import io
import json
import os
import re
import sys

# Third-party library imports
import folium
from folium.plugins import Draw, FastMarkerCluster
import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pyodbc
import pyqtgraph as pg
from collections import Counter
from pyproj import Transformer
# PyQt6 imports
from PyQt6.QtCore import (
    QAbstractTableModel, QSettings, QSize, Qt, QThread, pyqtSignal
)
from PyQt6.QtGui import QColor, QCursor, QFont, QIcon, QAction
from PyQt6.QtWebChannel import QWebChannel
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QButtonGroup, QCheckBox, QComboBox,
    QDialog, QDialogButtonBox, QDoubleSpinBox, QFileDialog, QFormLayout,
    QFrame, QGridLayout, QGroupBox, QHBoxLayout, QInputDialog, QLabel, QLineEdit,
    QListWidget, QListWidgetItem, QMainWindow, QMessageBox, QProgressBar,
    QPushButton, QRadioButton, QScrollArea, QSizePolicy, QSpinBox,
    QSplitter, QTabWidget, QTableView, QTableWidget, QTableWidgetItem,
    QTextBrowser, QTextEdit, QToolTip, QVBoxLayout, QWidget, QTreeWidget, QTreeWidgetItem, QMenu, QStackedWidget
)
from PyQt6.QtGui import QStandardItemModel
from PyQt6.QtGui import QColor, QBrush, QFont
from PyQt6.QtCore import Qt, QSize
from datetime import datetime

from PyQt6.QtGui import QIcon
from PyQt6.QtCore import QSize, QTimer, QPropertyAnimation, QEasingCurve
import os
from PyQt6.QtWidgets import QWidget, QVBoxLayout, QPushButton, QLabel, QFrame
from PyQt6.QtGui import QIcon, QPalette, QColor
from PyQt6.QtCore import Qt, QSize, QPropertyAnimation, QEasingCurve

import sys
import logging
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from PyQt6.QtWidgets import QWidget, QVBoxLayout
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWidgets import (QWidget, QSplitter, QTableWidget, QTableWidgetItem, 
                             QTabWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QLineEdit, QPushButton, QHeaderView)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtWebEngineWidgets import QWebEngineView
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from collections import Counter
from PyQt6.QtWidgets import QStatusBar
import plotly.express as px
from PyQt6.QtWidgets import QColorDialog
from plotly_resampler import FigureResampler, FigureWidgetResampler

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        # If not running as executable, use the script's directory
        base_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(base_path, relative_path)

class ExpandableSidebar(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.expanded = False
        self.animation = QPropertyAnimation(self, b"minimumWidth")
        self.animation.setEasingCurve(QEasingCurve.Type.InOutQuart)
        self.animation.setDuration(250)  # 250 ms for smooth animation
        
        # Set background color using QPalette
        palette = self.palette()
        palette.setColor(QPalette.ColorRole.Window, QColor("#1b1e23"))
        self.setPalette(palette)
        self.setAutoFillBackground(True)

    def enterEvent(self, event):
        self.expand()

    def leaveEvent(self, event):
        self.collapse()

    def expand(self):
        if not self.expanded:
            self.animation.setStartValue(self.width())
            self.animation.setEndValue(200)
            self.animation.start()
            self.expanded = True

    def collapse(self):
        if self.expanded:
            self.animation.setStartValue(self.width())
            self.animation.setEndValue(60)
            self.animation.start()
            self.expanded = False

class StackedPointsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Stacked Points Detected")
        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Stacked points (multiple data points with the same coordinates) have been detected. How would you like to proceed?"))

        self.button_group = QButtonGroup(self)
        
        self.stack_all = QRadioButton("Plot all points (including stacked)")
        self.stack_one = QRadioButton("Plot only one point per unique coordinate")
        self.cancel = QRadioButton("Cancel plotting")

        self.button_group.addButton(self.stack_all)
        self.button_group.addButton(self.stack_one)
        self.button_group.addButton(self.cancel)

        layout.addWidget(self.stack_all)
        layout.addWidget(self.stack_one)
        layout.addWidget(self.cancel)

        self.stack_all.setChecked(True)  # Default option

        self.ok_button = QPushButton("OK")
        self.ok_button.clicked.connect(self.accept)
        layout.addWidget(self.ok_button)
        
    def get_selection(self):
        if self.stack_all.isChecked():
            return "stack_all"
        elif self.stack_one.isChecked():
            return "stack_one"
        else:
            return "cancel"

class LayerManagementDialog(QDialog):
    def __init__(self, layers, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manage Layers")
        self.layers = layers
        self.removed_layers = []

        layout = QVBoxLayout(self)

        self.layer_list_widget = QListWidget(self)
        for layer in layers:
            self.layer_list_widget.addItem(layer)

        layout.addWidget(self.layer_list_widget)

        remove_button = QPushButton("Remove Selected", self)
        remove_button.clicked.connect(self.remove_selected_layers)
        layout.addWidget(remove_button)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def remove_selected_layers(self):
        selected_items = self.layer_list_widget.selectedItems()
        for item in selected_items:
            self.removed_layers.append(item.text())
            self.layer_list_widget.takeItem(self.layer_list_widget.row(item))

    def get_removed_layers(self):
        return self.removed_layers

class LayerCustomizationDialog(QDialog):
    def __init__(self, available_layers, shapefile_dfs, geojson_dfs, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Customize Layers")
        self.layer_styles = {}
        # Assuming shapefile_dfs and geojson_dfs are already dictionaries passed from the parent
        self.shapefile_dfs = shapefile_dfs
        self.geojson_dfs = geojson_dfs

        layout = QVBoxLayout(self)
        self.layer_cb = QComboBox(self)
        self.layer_cb.addItems(available_layers)
        self.layer_cb.currentIndexChanged.connect(self.load_layer_style)
        layout.addWidget(QLabel("Select Layer:"))
        layout.addWidget(self.layer_cb)

        # Color Input
        self.color_input = QLineEdit(self)
        layout.addWidget(QLabel("Polygon Color (hex):"))
        layout.addWidget(self.color_input)

        # Opacity Slider
        self.opacity_slider = QSlider(Qt.Orientation.Horizontal, self)
        self.opacity_slider.setMinimum(0)
        self.opacity_slider.setMaximum(100)
        self.opacity_slider.setValue(50)
        self.opacity_slider.setTickPosition(QSlider.TickPosition.TicksBelow)
        layout.addWidget(QLabel("Opacity:"))
        layout.addWidget(self.opacity_slider)

        # Label Selection
        self.label_cb = QComboBox(self)
        layout.addWidget(QLabel("Polygon Label:"))
        layout.addWidget(self.label_cb)

        # Column Table
        self.column_table = QTableWidget(self)
        layout.addWidget(QLabel("Available Columns:"))
        layout.addWidget(self.column_table)

        # Apply Button
        self.apply_btn = QPushButton("Apply Style", self)
        self.apply_btn.clicked.connect(self.apply_style)
        layout.addWidget(self.apply_btn)

        # Dialog Buttons
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self.load_layer_style()

    def load_layer_style(self):
        layer_name = self.layer_cb.currentText()
        is_shapefile = layer_name in self.shapefile_dfs
        gdf = self.shapefile_dfs.get(layer_name) if is_shapefile else self.geojson_dfs.get(layer_name)

        columns = list(gdf.columns)

        # Set up the table to display data
        self.column_table.setRowCount(gdf.shape[0])
        self.column_table.setColumnCount(gdf.shape[1])
        self.column_table.setHorizontalHeaderLabels(columns)

        # Populate the table with data from the DataFrame
        for row in range(gdf.shape[0]):
            for col in range(gdf.shape[1]):
                item = QTableWidgetItem(str(gdf.iloc[row, col]))
                self.column_table.setItem(row, col, item)

        style = self.layer_styles.get(layer_name, {'color': '#1f77b4', 'opacity': 0.5, 'label': None})
        self.color_input.setText(style['color'])
        self.opacity_slider.setValue(int(style['opacity'] * 100))
        self.label_cb.clear()
        self.label_cb.addItems(['None'] + columns)
        if style['label'] is not None:
            self.label_cb.setCurrentText(style['label'])
        else:
            self.label_cb.setCurrentIndex(0)

    def apply_style(self):
        layer_name = self.layer_cb.currentText()
        color = self.color_input.text()
        opacity = self.opacity_slider.value() / 100
        label = self.label_cb.currentText()
        if label == 'None':
            label = None
        self.layer_styles[layer_name] = {'color': color, 'opacity': opacity, 'label': label}

class MapView(QWidget):
    def __init__(self, parent=None, data_merger_app=None):
        super().__init__(parent)
        self.data_merger_app = data_merger_app
        self.layout = QHBoxLayout(self)
        self.geojson_dfs = []
        self.shapefile_dfs = []
        self.layer_styles = {}

        self.map_placeholder = QFrame(self)
        self.map_placeholder_layout = QHBoxLayout()
        self.map_placeholder.setLayout(self.map_placeholder_layout)

        self.channel = QWebChannel()
        self.initialize_map_view()

        self.create_map()
        self.create_widgets()  # Create all widgets before setting up UI
        self.setup_ui()
        self.first_plot = True

        self.selected_longitude = None
        self.selected_latitude = None
        self.data_frame = pd.DataFrame()
        self.selected_popup_column = None
        self.selected_popup_columns = []

    def create_widgets(self):
        # Create all necessary widgets here
        self.long_cb = QComboBox()
        self.lat_cb = QComboBox()
        self.popup_list = QListWidget()
        self.popup_list.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        
        self.update_btn = QPushButton("Update Map")
        self.update_btn.clicked.connect(self.update_selected_columns)
        self.update_btn.setEnabled(False)
        
        self.convert_utm_btn = QPushButton("Convert UTM to Lat/Long")
        self.convert_utm_btn.clicked.connect(self.convert_utm)
        
        self.load_geojson_btn = QPushButton("Load GeoJSON")
        self.load_geojson_btn.clicked.connect(self.load_geojson)
        self.load_shp_btn = QPushButton("Load SHP")
        self.load_shp_btn.clicked.connect(self.load_shp)
        self.join_btn = QPushButton("Apply Spatial Join")
        self.join_btn.clicked.connect(self.perform_spatial_join)
        
        self.manage_layers_btn = QPushButton("Manage Layers")
        self.manage_layers_btn.clicked.connect(self.manage_layers)
        self.customize_layers_btn = QPushButton("Customize Layers")
        self.customize_layers_btn.clicked.connect(self.open_layer_customization)
        self.revert_btn = QPushButton("Revert Changes")
        self.revert_btn.clicked.connect(self.revert_changes)
        
        self.export_map_btn = QPushButton("Export Map")
        self.export_map_btn.clicked.connect(self.export_map)

    def setup_ui(self):
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(5, 5, 5, 5)
        left_layout.setSpacing(5)

        # Coordinate Selection
        coord_group = QGroupBox("Coordinate Selection")
        coord_layout = QGridLayout(coord_group)
        coord_layout.setContentsMargins(5, 5, 5, 5)
        coord_layout.setSpacing(5)
        coord_layout.addWidget(QLabel("Long:"), 0, 0)
        coord_layout.addWidget(self.long_cb, 0, 1)
        coord_layout.addWidget(QLabel("Lat:"), 1, 0)
        coord_layout.addWidget(self.lat_cb, 1, 1)
        coord_layout.addWidget(self.convert_utm_btn, 2, 0, 1, 2)
        left_layout.addWidget(coord_group)

        # Map Creation and Popup Selection
        map_popup_group = QGroupBox("Map Creation and Popup Selection")
        map_popup_layout = QVBoxLayout(map_popup_group)
        map_popup_layout.setContentsMargins(5, 5, 5, 5)
        map_popup_layout.setSpacing(5)
        map_popup_layout.addWidget(QLabel("Select Popup Content:"))
        map_popup_layout.addWidget(self.popup_list)
        self.popup_list.setMaximumHeight(100)  # Limit the height of the list
        map_popup_layout.addWidget(self.update_btn)
        left_layout.addWidget(map_popup_group)

        # Layer Management group
        layer_management_group = QGroupBox("Layer Management")
        layer_management_layout = QVBoxLayout(layer_management_group)
        layer_management_layout.setContentsMargins(5, 5, 5, 5)
        layer_management_layout.setSpacing(5)
        layer_management_layout.addWidget(self.load_geojson_btn)
        layer_management_layout.addWidget(self.load_shp_btn)
        layer_management_layout.addWidget(self.join_btn)
        layer_management_layout.addWidget(self.revert_btn)
        left_layout.addWidget(layer_management_group)

        # Map Customization group
        map_customization_group = QGroupBox("Map Customization")
        map_customization_layout = QVBoxLayout(map_customization_group)
        map_customization_layout.setContentsMargins(5, 5, 5, 5)
        map_customization_layout.setSpacing(5)
        map_customization_layout.addWidget(self.manage_layers_btn)
        map_customization_layout.addWidget(self.customize_layers_btn)
        map_customization_layout.addWidget(self.export_map_btn)
        left_layout.addWidget(map_customization_group)

        left_layout.addStretch(1)

        # Use QSplitter for resizable layout
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(left_widget)
        splitter.addWidget(self.map_placeholder)
        
        # Set initial sizes: left side 150px, right side takes the rest
        splitter.setSizes([150, self.width() - 150])

        self.layout.addWidget(splitter)

        # Make sure the left side doesn't expand too much when resizing
        left_widget.setMaximumWidth(500)

    def initialize_map_view(self):
        self.map_view = QWebEngineView(self.map_placeholder)
        self.map_view.setMinimumSize(600, 400)  # Adjust minimum size as needed
        self.map_view.setSizePolicy(QSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding))
        self.map_placeholder_layout.addWidget(self.map_view)
        self.map_view.page().setWebChannel(self.channel)
        self.map_view.page().profile().downloadRequested.connect(self.handle_downloadRequested)

    def create_long_lat_group(self):
        group_box = QGroupBox("Longitude/Latitude/Popup Selection")
        layout = QVBoxLayout(group_box)
        coord_layout = QHBoxLayout()

        self.long_cb = QComboBox()
        self.lat_cb = QComboBox()
        self.popup_list = QListWidget()
        self.popup_list.setSelectionMode(QListWidget.SelectionMode.MultiSelection)

        coord_layout.addWidget(QLabel("Select Longitude:"))
        coord_layout.addWidget(self.long_cb)
        coord_layout.addWidget(QLabel("Select Latitude:"))
        coord_layout.addWidget(self.lat_cb)
        layout.addLayout(coord_layout)

        layout.addWidget(QLabel("Select Popup Content:"))
        layout.addWidget(self.popup_list)

        # Create a horizontal layout for the buttons
        buttons_layout = QHBoxLayout()

        self.update_btn = QPushButton("Update Map")
        self.update_btn.clicked.connect(self.update_selected_columns)
        self.update_btn.setEnabled(False)
        buttons_layout.addWidget(self.update_btn)  # Add to the horizontal layout

        self.convert_utm_btn = QPushButton("Convert UTM to Lat/Long")
        self.convert_utm_btn.clicked.connect(self.convert_utm)
        buttons_layout.addWidget(self.convert_utm_btn)  # Add to the horizontal layout

        layout.addLayout(buttons_layout)  # Add the horizontal layout to the main vertical layout

        return group_box

    def create_data_load_group(self):
        group_box = QGroupBox("Data Load")
        layout = QVBoxLayout(group_box)

        self.load_geojson_btn = QPushButton("Load GeoJSON")
        self.load_geojson_btn.clicked.connect(self.load_geojson)
        self.load_shp_btn = QPushButton("Load SHP")
        self.load_shp_btn.clicked.connect(self.load_shp)
        self.join_btn = QPushButton("Apply Spatial Join")
        self.join_btn.clicked.connect(self.perform_spatial_join)

        layout.addWidget(self.load_geojson_btn)
        layout.addWidget(self.load_shp_btn)
        layout.addWidget(self.join_btn)

        return group_box

    def create_map_actions_group(self):
        group_box = QGroupBox("Map Actions")
        layout = QVBoxLayout(group_box)

        self.manage_layers_btn = QPushButton("Manage Layers")
        self.manage_layers_btn.clicked.connect(self.manage_layers)
        self.customize_layers_btn = QPushButton("Customize Layers")
        self.customize_layers_btn.clicked.connect(self.open_layer_customization)
        self.revert_btn = QPushButton("Revert Changes")
        self.revert_btn.clicked.connect(self.revert_changes)

        layout.addWidget(self.manage_layers_btn)
        layout.addWidget(self.customize_layers_btn)
        layout.addWidget(self.revert_btn)

        return group_box

    def create_export_buttons_group(self):
        group_box = QGroupBox("Export Options")
        layout = QHBoxLayout(group_box)
        self.export_map_btn = QPushButton("Export Map")
        self.export_map_btn.clicked.connect(self.export_map)
        layout.addWidget(self.export_map_btn)
        return group_box

    def reset_map(self):
        # Reset the folium map instance
        self.map = folium.Map(location=[56, 10], zoom_start=5, tiles='OpenStreetMap')
        Draw(
            export=True,
            filename="my_data.geojson",
            position="topleft",
            draw_options={"polyline": True, "rectangle": False, "circle": False, "circlemarker": False},
            edit_options={"poly": {"allowIntersection": False}}
        ).add_to(self.map)
        self.update_map()  # Update the map in the QWebEngineView

    def set_data_frame(self, data_frame):
        self.data_frame = data_frame
        if self.data_frame is not None:
            self.long_cb.clear()
            self.lat_cb.clear()
            self.popup_list.clear()  # Clear the popup list
            self.long_cb.addItems(self.data_frame.columns)
            self.lat_cb.addItems(self.data_frame.columns)
            for column in self.data_frame.columns:
                item = QListWidgetItem(column)
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)  # Add checkbox
                item.setCheckState(Qt.CheckState.Checked if column in self.selected_popup_columns else Qt.CheckState.Unchecked)
                self.popup_list.addItem(item)
            # Re-select previously selected columns if they still exist
            if self.selected_longitude in self.data_frame.columns:
                self.long_cb.setCurrentText(self.selected_longitude)
            if self.selected_latitude in self.data_frame.columns:
                self.lat_cb.setCurrentText(self.selected_latitude)
            self.update_btn.setEnabled(True)
        else:
            self.long_cb.clear()
            self.lat_cb.clear()
            self.popup_list.clear()  # Clear the popup list
            self.long_cb.addItem("Select data to enable")
            self.lat_cb.addItem("Select data to enable")
            self.popup_list.addItem("Select data to enable")  # Disable popup content selection
            self.update_btn.setEnabled(False)

    def update_selected_columns(self):
        self.selected_popup_columns = [self.popup_list.item(i).text() for i in range(self.popup_list.count()) if self.popup_list.item(i).checkState() == Qt.CheckState.Checked]
        self.plot_points()
        
    def create_map(self):
        self.map = folium.Map(location=[56, 10], zoom_start=5)
        Draw(
            export=True,
            filename="my_data.geojson",
            position="topleft",
            draw_options={"polyline": True, "rectangle": False, "circle": False, "circlemarker": False},
            edit_options={"poly": {"allowIntersection": False}}
        ).add_to(self.map)
        self.update_map()

    def handle_downloadRequested(self, item):
        options = QFileDialog.Option.DontUseNativeDialog
        fileName, _ = QFileDialog.getSaveFileName(self, "Save File", item.suggestedFileName(), "GeoJSON Files (*.geojson)", options=options)
        if fileName:
            item.setDownloadDirectory(os.path.dirname(fileName))
            item.setDownloadFileName(os.path.basename(fileName))
            item.accept()

    def update_map(self):
        data = io.BytesIO()
        self.map.save(data, close_file=False)
        data.seek(0)
        self.map_view.setHtml(data.getvalue().decode())

    def add_layer_to_map(self, gdf, layer_name="Layer", color="#1f77b4", opacity=0.5, label=None):
        style = self.layer_styles.get(layer_name, {'color': color, 'opacity': opacity, 'label': label})
        folium.GeoJson(
            gdf,
            name=layer_name,
            style_function=lambda feature: {
                'fillColor': style['color'],
                'color': 'black',
                'weight': 1,
                'fillOpacity': style['opacity']
            },
            tooltip=folium.GeoJsonTooltip(fields=[style['label']]) if style['label'] else None
        ).add_to(self.map)
        self.update_map()
        print(f"Layer '{layer_name}' added to the map with custom styles.")
                
    def open_layer_customization(self):
        available_layers = [layer_name for _, layer_name in self.shapefile_dfs] + [layer_name for _, layer_name in self.geojson_dfs]
        # Ensure dictionaries are correctly used:
        shapefile_dict = {name: df for df, name in self.shapefile_dfs}
        geojson_dict = {name: df for df, name in self.geojson_dfs}

        dialog = LayerCustomizationDialog(available_layers, shapefile_dict, geojson_dict, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.layer_styles = dialog.layer_styles
            self.reset_map()
            self.plot_points()
            self.add_all_shapefiles()
            self.add_all_geojson()
            print("Layers customized and updated.")

    def add_shapefile_to_map(self, gdf, layer_name="Shapefile Layer"):
        self.add_layer_to_map(gdf, layer_name, color="#228B22")

    def add_geojson_to_map(self, gdf, layer_name="GeoJSON Layer"):
        self.add_layer_to_map(gdf, layer_name, color="#1f77b4")

    def add_all_shapefiles(self):
        for gdf, layer_name in self.shapefile_dfs:
            self.add_shapefile_to_map(gdf, layer_name)

    def add_all_geojson(self):
        for gdf, layer_name in self.geojson_dfs:
            self.add_geojson_to_map(gdf, layer_name)

    def load_shp(self):
        fileNames, _ = QFileDialog.getOpenFileNames(self, "Open Shapefile", "", "Shapefiles (*.shp)")
        for fileName in fileNames:
            gdf = gpd.read_file(fileName)
            if gdf.crs is None or gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs("EPSG:4326")
            layer_name = f"{os.path.splitext(os.path.basename(fileName))[0]} Layer {len(self.shapefile_dfs) + 1}"
            self.shapefile_dfs.append((gdf, layer_name))
            self.add_shapefile_to_map(gdf, layer_name)

    def load_geojson(self):
        fileNames, _ = QFileDialog.getOpenFileNames(self, "Open GeoJSON File", "", "GeoJSON Files (*.geojson)")
        for fileName in fileNames:
            gdf = gpd.read_file(fileName)
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)
            layer_name = f"{os.path.splitext(os.path.basename(fileName))[0]} Layer {len(self.geojson_dfs) + 1}"
            self.geojson_dfs.append((gdf, layer_name))
            self.add_geojson_to_map(gdf, layer_name)


    def manage_layers(self):
        layers = [layer_name for _, layer_name in self.shapefile_dfs] + [layer_name for _, layer_name in self.geojson_dfs]
        dialog = LayerManagementDialog(layers, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            removed_layers = dialog.get_removed_layers()
            self.shapefile_dfs = [gdf_tup for gdf_tup in self.shapefile_dfs if gdf_tup[1] not in removed_layers]
            self.geojson_dfs = [gdf_tup for gdf_tup in self.geojson_dfs if gdf_tup[1] not in removed_layers]

            self.reset_map()
            self.perform_spatial_join()

    def perform_spatial_join(self):
        if hasattr(self, 'geo_df'):
            if self.geo_df.crs is None:
                self.geo_df.set_crs("EPSG:4326", inplace=True)
        
            if not hasattr(self, 'original_df'):  # Only set original_df if it hasn't been set yet
                self.original_df = self.data_merger_app.df1.copy()

            # Combine all GeoJSON and shapefiles into a unified layer
            shapefile_gdfs = [gdf for gdf, _ in self.shapefile_dfs]
            geojson_gdfs = [gdf for gdf, _ in self.geojson_dfs]
            all_layers = gpd.GeoDataFrame(pd.concat(shapefile_gdfs + geojson_gdfs, ignore_index=True))
            all_layers = all_layers.set_crs("EPSG:4326", allow_override=True)

            # Avoid index name conflicts
            for col in ['index_left', 'index_right']:
                if col in self.geo_df.columns:
                    self.geo_df.rename(columns={col: f'{col}_old'}, inplace=True)
                if col in all_layers.columns:
                    all_layers.rename(columns={col: f'{col}_old'}, inplace=True)

            # Perform the spatial join
            joined_df = gpd.sjoin(self.geo_df, all_layers, how="inner", op='intersects')
            self.geo_df = joined_df
            self.data_merger_app.df1 = self.geo_df

            if hasattr(self.data_merger_app, 'display_data'):
                self.data_merger_app.display_data(self.data_merger_app.df1, 'data1', 'Joined Data.geojson')

            # Update the DataFrame in the UI components
            self.set_data_frame(self.geo_df)

            self.plot_points()
            self.add_all_shapefiles()
            self.add_all_geojson()
            print("Spatial join performed with all loaded GeoJSON and SHP files.")

    def revert_changes(self):
        if hasattr(self, 'original_df'):
            self.data_merger_app.df1 = self.original_df.copy()  # Restore the original DataFrame from the backup
            self.set_data_frame(self.original_df)  # Update the data frame in the UI components

            if hasattr(self.data_merger_app, 'display_data'):
                self.data_merger_app.display_data(self.original_df, 'data1', 'Original Data.geojson')  # Optional: Display the original data

            self.reset_map()  # Reset the map to remove any plotted points or layers
            self.plot_points()  # Re-plot the original points
            self.add_all_shapefiles()  # Re-add all shapefiles to the map
            self.add_all_geojson()  # Re-add all GeoJSON layers to the map
            print("Changes reverted to original data.")

            QMessageBox.information(self, "Revert Successful", "All changes have been reverted to the original data.")

    def convert_utm(self):
        utm_x_col, ok_x = QInputDialog.getItem(self, "Select UTM X Column", "Column:", self.data_frame.columns.tolist(), 0, False)
        if not ok_x:
            return
        utm_y_col, ok_y = QInputDialog.getItem(self, "Select UTM Y Column", "Column:", self.data_frame.columns.tolist(), 0, False)
        if not ok_y:
            return

        # Ensure columns are numeric and filter out NaN values
        try:
            self.data_frame[utm_x_col] = pd.to_numeric(self.data_frame[utm_x_col], errors='coerce')
            self.data_frame[utm_y_col] = pd.to_numeric(self.data_frame[utm_y_col], errors='coerce')
        except Exception as e:
            QMessageBox.warning(self, "Conversion Error", f"Error converting UTM columns to numeric: {str(e)}")
            return

        # Filter out rows with NaN values in either UTM X or Y columns
        valid_data = self.data_frame.dropna(subset=[utm_x_col, utm_y_col])

        # If there's no valid data to process, alert the user and exit the function
        if valid_data.empty:
            QMessageBox.warning(self, "No Data", "There are no rows with valid UTM coordinates to convert.")
            return

        # Perform the coordinate transformation
        transformer = Transformer.from_crs(f'+proj=utm +zone=32 +ellps=WGS84 +datum=WGS84 +units=m +no_defs', 'EPSG:4326', always_xy=True)
        lon, lat = transformer.transform(valid_data[utm_x_col].values, valid_data[utm_y_col].values)

        # Update the DataFrame only for the rows with valid data
        self.data_frame.loc[valid_data.index, 'longitude'] = lon
        self.data_frame.loc[valid_data.index, 'latitude'] = lat

        QMessageBox.information(self, "Conversion Complete", "UTM coordinates have been converted to longitude and latitude.")
        self.set_data_frame(self.data_frame)  # Refresh the DataFrame in the UI if necessary

    def export_map(self):
        filename = QFileDialog.getSaveFileName(self, "Export Map", "", "HTML Files (*.html)")
        if filename[0]:
            self.map.save(filename[0])
            QMessageBox.information(self, "Export Successful", f"Map has been successfully exported to {filename[0]}")
            
    def plot_points(self):
        if not self.first_plot:
            self.reset_map()
        else:
            self.first_plot = False
        if self.data_merger_app.df1 is not None:
            long_col = self.long_cb.currentText()
            lat_col = self.lat_cb.currentText()
            self.selected_longitude = long_col
            self.selected_latitude = lat_col
            
            if not self.selected_popup_columns:
                QMessageBox.information(self, "Popup Configuration", "Please select one or more columns for popup content.")
                return

            filtered_df = self.data_merger_app.df1.dropna(subset=[long_col, lat_col])
            
            # Check for stacked points
            unique_coords = filtered_df.groupby([long_col, lat_col]).size().reset_index(name='count')
            has_stacked_points = (unique_coords['count'] > 1).any()

            if has_stacked_points:
                dialog = StackedPointsDialog(self)
                if dialog.exec() == QDialog.DialogCode.Accepted:
                    selection = dialog.get_selection()
                    if selection == "stack_one":
                        filtered_df = filtered_df.drop_duplicates(subset=[long_col, lat_col])
                    elif selection == "cancel":
                        return  # Exit the method if user chooses to cancel
                else:
                    return  # Exit if dialog is cancelled

            self.geo_df = gpd.GeoDataFrame(filtered_df, geometry=gpd.points_from_xy(filtered_df[long_col], filtered_df[lat_col]))

            def format_popup(row):
                return '<br>'.join(f"{col}: {row[col] if pd.notna(row[col]) else 'N/A'}" for col in self.selected_popup_columns)
            
            popup_data = filtered_df.apply(format_popup, axis=1)
            locations_with_popup = filtered_df[[lat_col, long_col]].values.tolist()
            popup_texts = popup_data.tolist()

            callback = ('function (row) {'
                        'var marker = L.marker(new L.LatLng(row[0], row[1]));'
                        'var popup = L.popup({maxWidth: "300"});'
                        'popup.setContent(row[2]);'
                        'marker.bindPopup(popup);'
                        'return marker;};')
            
            locations_with_popup = [list(loc) + [pop] for loc, pop in zip(locations_with_popup, popup_texts)]

            marker_cluster = FastMarkerCluster(
                locations_with_popup,
                callback=callback,
                control=False,
                maxClusterRadius=40,
                disableClusteringAtZoom=15
            ).add_to(self.map)

            if not filtered_df.empty:
                self.map.fit_bounds([
                    [filtered_df[lat_col].min(), filtered_df[long_col].min()],
                    [filtered_df[lat_col].max(), filtered_df[long_col].max()]
                ])
            self.update_map()
            self.add_all_geojson()
            self.add_all_shapefiles()
            
class PlottingWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.df = None
        self.subplot_types_layout = QGridLayout()  # Initialize here
        self.setup_ui()
        self.use_resampler = True  # Flag to enable/disable resampler
        self.max_n_samples = 10000  # Maximum number of samples to display
                    
    def setup_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # Create a splitter for left panel and plot view
        splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(splitter)

        # Left panel (scrollable)
        left_panel = QScrollArea()
        left_panel.setWidgetResizable(True)
        left_panel_widget = QWidget()
        left_panel_layout = QVBoxLayout(left_panel_widget)
        left_panel.setWidget(left_panel_widget)

        # Add groups to left panel
        left_panel_layout.addWidget(self.create_plot_type_group())
        left_panel_layout.addWidget(self.create_data_selection_group())
        self.subplot_options_group = self.create_subplot_options_group()
        left_panel_layout.addWidget(self.subplot_options_group)
        self.combined_plot_options_group = self.create_combined_plot_options_group()
        left_panel_layout.addWidget(self.combined_plot_options_group)
        self.plot_customization_group = self.create_plot_customization_group()
        left_panel_layout.addWidget(self.plot_customization_group)
        
        # Add generate plot button
        self.generate_plot_button = QPushButton("Generate Plot")
        self.generate_plot_button.clicked.connect(self.generate_plot)
        left_panel_layout.addWidget(self.generate_plot_button)

        # Add stretch to push groups to the top
        left_panel_layout.addStretch(1)

        # Right panel (plot view)
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        self.plot_view = QWebEngineView()
        right_layout.addWidget(self.plot_view)

        # Add panels to splitter
        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)

        # Set initial sizes (adjust as needed)
        splitter.setSizes([300, 700])

        # Add status bar
        self.status_bar = QStatusBar()
        self.status_bar.setSizeGripEnabled(False)
        self.status_bar.setFixedHeight(20)
        self.status_bar.setStyleSheet("QStatusBar { background-color: #f0f0f0; }")
        main_layout.addWidget(self.status_bar)

        # Initialize visibility of options
        self.update_customization_options()

        # Connect plot type change to update options
        self.plot_type_selector.currentIndexChanged.connect(self.update_customization_options)

        # Initialize subplot type selectors
        self.update_subplot_type_selectors()

        # Connect y_axis_selector to update_combined_plot_table
        if hasattr(self, 'y_axis_selector'):
            self.y_axis_selector.itemSelectionChanged.connect(self.update_combined_plot_table)

        # Initialize combined plot table
        self.update_combined_plot_table()

    def create_plot_customization_group(self):
        self.customization_group = QGroupBox("Plot Customization")
        self.customization_layout = QVBoxLayout(self.customization_group)
        
        # Common options
        self.title_input = QLineEdit()
        self.title_input.setPlaceholderText("Enter plot title")
        self.customization_layout.addWidget(QLabel("Plot Title:"))
        self.customization_layout.addWidget(self.title_input)
        
        # Bar chart options
        self.bar_options_widget = QWidget()
        bar_options_layout = QVBoxLayout(self.bar_options_widget)
        
        self.bar_orientation = QComboBox()
        self.bar_orientation.addItems(["Vertical", "Horizontal"])
        bar_options_layout.addWidget(QLabel("Bar Orientation:"))
        bar_options_layout.addWidget(self.bar_orientation)
        
        self.bar_mode = QComboBox()
        self.bar_mode.addItems(["Group", "Stack"])
        bar_options_layout.addWidget(QLabel("Bar Mode:"))
        bar_options_layout.addWidget(self.bar_mode)
        
        self.sort_bars = QCheckBox("Sort Bars")
        bar_options_layout.addWidget(self.sort_bars)
        
        # Line plot options
        self.line_options_widget = QWidget()
        line_options_layout = QVBoxLayout(self.line_options_widget)
        
        self.line_style = QComboBox()
        self.line_style.addItems(["Lines", "Lines+Markers", "Markers"])
        line_options_layout.addWidget(QLabel("Line Style:"))
        line_options_layout.addWidget(self.line_style)
        
        self.smoothing = QCheckBox("Apply Smoothing")
        line_options_layout.addWidget(self.smoothing)
        
        self.secondary_y = QCheckBox("Use Secondary Y-axis")
        line_options_layout.addWidget(self.secondary_y)
        
        # Add options to main layout
        self.customization_layout.addWidget(self.bar_options_widget)
        self.customization_layout.addWidget(self.line_options_widget)
        
        # Color customization
        self.color_button = QPushButton("Customize Colors")
        self.color_button.clicked.connect(self.customize_colors)
        self.customization_layout.addWidget(self.color_button)
        
        self.show_legend = QCheckBox("Show Legend")
        self.show_legend.setChecked(True)
        self.customization_layout.addWidget(self.show_legend)
            
        self.animate_checkbox = QCheckBox("Animate Plot")
        self.animate_checkbox.setToolTip("Animate plot for time-series data")
        self.customization_layout.addWidget(self.animate_checkbox)
        
        return self.customization_group

    def update_customization_options(self):
        plot_type = self.plot_type_selector.currentText()
        self.bar_options_widget.setVisible(plot_type in ["Bar Chart", "Combined Plot"])
        self.line_options_widget.setVisible(plot_type in ["Line Plot", "Combined Plot"])
        self.subplot_options_group.setVisible(plot_type == "Subplots")
        self.combined_plot_options_group.setVisible(plot_type == "Combined Plot")

    def create_plot_type_group(self):
        group = QGroupBox("Plot Type")
        layout = QVBoxLayout(group)
        self.plot_type_selector = QComboBox()
        self.plot_type_selector.addItems(["Bar Chart", "Line Plot", "Subplots", "Combined Plot"])
        self.plot_type_selector.currentIndexChanged.connect(self.update_customization_options)
        layout.addWidget(self.plot_type_selector)
        return group
                
    def create_subplot_options_group(self):
        group = QGroupBox("Subplot Options")
        layout = QVBoxLayout(group)
        
        self.subplot_rows = QSpinBox()
        self.subplot_rows.setMinimum(1)
        self.subplot_rows.setMaximum(4)
        layout.addWidget(QLabel("Number of Rows:"))
        layout.addWidget(self.subplot_rows)
        
        self.subplot_cols = QSpinBox()
        self.subplot_cols.setMinimum(1)
        self.subplot_cols.setMaximum(4)
        layout.addWidget(QLabel("Number of Columns:"))
        layout.addWidget(self.subplot_cols)
        
        layout.addWidget(QLabel("Subplot Types:"))
        layout.addLayout(self.subplot_types_layout)  # Use the pre-initialized layout
        
        self.subplot_rows.valueChanged.connect(self.update_subplot_type_selectors)
        self.subplot_cols.valueChanged.connect(self.update_subplot_type_selectors)
        
        return group
    
    def update_subplot_type_selectors(self):
        # Clear existing layout
        for i in reversed(range(self.subplot_types_layout.count())): 
            widget = self.subplot_types_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)
        
        self.subplot_types = []
        
        rows = self.subplot_rows.value()
        cols = self.subplot_cols.value()
        
        for i in range(rows * cols):
            selector = QComboBox()
            selector.addItems(["Line Plot", "Bar Chart"])
            self.subplot_types.append(selector)
            self.subplot_types_layout.addWidget(QLabel(f"Subplot {i+1}:"), i // cols, (i % cols) * 2)
            self.subplot_types_layout.addWidget(selector, i // cols, (i % cols) * 2 + 1)
        
    def create_data_selection_group(self):
        group = QGroupBox("Data Selection")
        layout = QVBoxLayout(group)
        
        layout.addWidget(QLabel("X-axis:"))
        self.x_axis_selector = QComboBox()
        layout.addWidget(self.x_axis_selector)
        
        layout.addWidget(QLabel("Y-axis:"))
        self.y_axis_selector = QListWidget()
        self.y_axis_selector.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        layout.addWidget(self.y_axis_selector)
        
        self.group_by_label = QLabel("Group by (optional):")
        layout.addWidget(self.group_by_label)
        self.group_by_selector = QComboBox()
        layout.addWidget(self.group_by_selector)
        
        self.generate_plot_button = QPushButton("Generate Plot")
        self.generate_plot_button.clicked.connect(self.generate_plot)
        layout.addWidget(self.generate_plot_button)
        
        return group
            
    def create_combined_plot_options_group(self):
        group = QGroupBox("Combined Plot Options")
        layout = QVBoxLayout(group)
        
        self.combined_plot_table = QTableWidget()
        self.combined_plot_table.setColumnCount(3)
        self.combined_plot_table.setHorizontalHeaderLabels(["Column", "Plot Type", "Y-Axis"])
        self.combined_plot_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.combined_plot_table.setMinimumHeight(150)  # Set a minimum height
        layout.addWidget(self.combined_plot_table)
        
        return group

    def update_combined_plot_table(self):
        if hasattr(self, 'y_axis_selector') and self.y_axis_selector.selectedItems():
            self.combined_plot_table.setRowCount(len(self.y_axis_selector.selectedItems()))
            for i, item in enumerate(self.y_axis_selector.selectedItems()):
                column_name = item.text()
                self.combined_plot_table.setItem(i, 0, QTableWidgetItem(column_name))
                
                plot_type_combo = QComboBox()
                plot_type_combo.addItems(["Line Plot", "Bar Chart"])
                self.combined_plot_table.setCellWidget(i, 1, plot_type_combo)
                
                y_axis_combo = QComboBox()
                y_axis_combo.addItems(["Primary", "Secondary"])
                self.combined_plot_table.setCellWidget(i, 2, y_axis_combo)
        else:
            self.combined_plot_table.setRowCount(0)

    def set_dataframe(self, df):
        self.df = df
        self.update_column_selectors()

    def update_column_selectors(self):
        self.x_axis_selector.clear()
        self.y_axis_selector.clear()
        self.group_by_selector.clear()
        
        if self.df is not None:
            self.group_by_selector.addItem("None")
            for column in self.df.columns:
                self.x_axis_selector.addItem(column)
                self.y_axis_selector.addItem(column)
                self.group_by_selector.addItem(column)
        
        self.generate_plot_button.setEnabled(self.df is not None)

    def generate_plot(self):
        if self.df is None:
            self.show_error("No data loaded. Please load data first.")
            return

        plot_type = self.plot_type_selector.currentText()
        x_column = self.x_axis_selector.currentText()
        y_columns = [item.text() for item in self.y_axis_selector.selectedItems()]
        group_by = self.group_by_selector.currentText()
        group_by = None if group_by == "None" else group_by

        if not y_columns:
            self.show_error("Please select at least one Y-axis column.")
            return

        try:
            if plot_type == "Bar Chart":
                self.create_bar_chart(x_column, y_columns, group_by)
            elif plot_type == "Line Plot":
                self.create_line_plot(x_column, y_columns, group_by)
            elif plot_type == "Subplots":
                self.create_subplots(x_column, y_columns, group_by)
            elif plot_type == "Combined Plot":
                self.create_combined_plot(x_column, y_columns, group_by)
            
            self.show_success("Plot generated successfully!")
        except Exception as e:
            self.show_error(f"Error generating plot: {str(e)}")
            
    def create_subplots(self, x_column, y_columns, group_by=None):
        rows = self.subplot_rows.value()
        cols = self.subplot_cols.value()
        
        fig = make_subplots(
            rows=rows, 
            cols=cols, 
            subplot_titles=y_columns[:rows*cols],
            vertical_spacing=0.1,
            horizontal_spacing=0.05
        )

        for i, y_column in enumerate(y_columns[:rows*cols]):
            row = i // cols + 1
            col = i % cols + 1
            
            # Default to Line Plot if subplot_types is not set
            plot_type = "Line Plot"
            if hasattr(self, 'subplot_types') and i < len(self.subplot_types):
                plot_type = self.subplot_types[i].currentText()
            
            if plot_type == "Bar Chart":
                subplot_fig = self.create_bar_chart(x_column, [y_column], group_by, for_subplot=True)
            else:  # Line Plot
                subplot_fig = self.create_line_plot(x_column, [y_column], group_by, for_subplot=True)
            
            # Add traces from subplot_fig to the main figure
            for trace in subplot_fig.data:
                fig.add_trace(trace, row=row, col=col)
            
            # Copy over the axis properties
            fig.update_xaxes(subplot_fig.layout.xaxis, row=row, col=col)
            fig.update_yaxes(subplot_fig.layout.yaxis, row=row, col=col)

        fig.update_layout(
            height=300*rows,
            width=400*cols,
            title_text="Subplots",
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=50, r=50, t=50, b=50)
        )

        fig.update_layout(template="plotly_white")
        fig.update_annotations(font_size=10)

        self.plot_view.setHtml(fig.to_html(include_plotlyjs='cdn', config={'responsive': True}))


    def create_combined_plot(self, x_column, y_columns, group_by=None):
        if self.use_resampler:
            fig = FigureResampler(go.Figure())
        else:
            fig = go.Figure()

        primary_yaxis = []
        secondary_yaxis = []

        sorted_df = self.df.sort_values(by=x_column)
        x_data = sorted_df[x_column].astype(str) if pd.api.types.is_datetime64_any_dtype(sorted_df[x_column]) else sorted_df[x_column]

        for row in range(self.combined_plot_table.rowCount()):
            y_column = self.combined_plot_table.item(row, 0).text()
            plot_type = self.combined_plot_table.cellWidget(row, 1).currentText()
            y_axis = self.combined_plot_table.cellWidget(row, 2).currentText()

            if plot_type == "Bar Chart":
                if self.use_resampler:
                    fig.add_trace(
                        go.Bar(name=y_column),
                        hf_x=x_data,
                        hf_y=sorted_df[y_column],
                        max_n_samples=self.max_n_samples
                    )
                else:
                    trace = go.Bar(x=x_data, y=sorted_df[y_column], name=y_column)
            else:  # Line Plot
                if self.use_resampler:
                    fig.add_trace(
                        go.Scattergl(name=y_column, mode='lines'),
                        hf_x=x_data,
                        hf_y=sorted_df[y_column],
                        max_n_samples=self.max_n_samples
                    )
                else:
                    trace = go.Scatter(x=x_data, y=sorted_df[y_column], name=y_column, mode='lines')

            if y_axis == "Secondary":
                if not self.use_resampler:
                    trace.update(yaxis="y2")
                secondary_yaxis.append(y_column)
            else:
                primary_yaxis.append(y_column)

            if not self.use_resampler:
                fig.add_trace(trace)

        # Update layout with two y-axes
        fig.update_layout(
            title=self.title_input.text() or f"Combined Plot: {', '.join(y_columns)} by {x_column}",
            xaxis_title=x_column,
            yaxis=dict(title=", ".join(primary_yaxis) if primary_yaxis else ""),
            yaxis2=dict(title=", ".join(secondary_yaxis) if secondary_yaxis else "",
                        overlaying="y",
                        side="right"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        # Improve axis formatting
        if pd.api.types.is_numeric_dtype(sorted_df[x_column]):
            fig.update_xaxes(tickformat=",.0f")
        elif pd.api.types.is_datetime64_any_dtype(sorted_df[x_column]):
            fig.update_xaxes(tickformat="%Y-%m-%d")

        if self.use_resampler:
            self.plot_view.setHtml(fig.to_html(include_plotlyjs='cdn', config={'responsive': True}))
        else:
            self.plot_view.setHtml(fig.to_html(include_plotlyjs='cdn'))
        
    def customize_colors(self):
        color_dialog = QColorDialog(self)
        if color_dialog.exec() == QDialog.DialogCode.Accepted:
            color = color_dialog.selectedColor()
            if color.isValid():
                self.custom_color = color.name()
                self.show_success(f"Custom color set to {self.custom_color}")

    def create_bar_chart(self, x_column, y_columns, group_by=None, for_subplot=False):
        fig = go.Figure()

        orientation = 'v' if self.bar_orientation.currentText() == "Vertical" else 'h'
        bar_mode = self.bar_mode.currentText().lower()

        # Color scale
        color_scale = px.colors.qualitative.Plotly

        # Sort and aggregate data
        sorted_df = self.df.sort_values(by=x_column)
        
        # Limit the number of bars to display
        max_bars = 6000  # Adjust this value as needed
        
        if group_by and group_by != "None":
            for i, y_column in enumerate(y_columns):
                grouped = sorted_df.groupby([x_column, group_by])[y_column].mean().reset_index()
                top_categories = grouped.groupby(x_column)[y_column].sum().nlargest(max_bars).index
                grouped = grouped[grouped[x_column].isin(top_categories)]
                
                for j, group in enumerate(grouped[group_by].unique()):
                    group_data = grouped[grouped[group_by] == group]
                    if orientation == 'v':
                        x_data = group_data[x_column]
                        y_data = group_data[y_column]
                    else:
                        x_data = group_data[y_column]
                        y_data = group_data[x_column]
                    
                    if self.sort_bars.isChecked():
                        sorted_indices = y_data.argsort()
                        x_data = x_data.iloc[sorted_indices]
                        y_data = y_data.iloc[sorted_indices]

                    fig.add_trace(go.Bar(
                        x=x_data,
                        y=y_data,
                        name=f"{y_column} - {group}",
                        orientation=orientation,
                        marker_color=color_scale[i * len(grouped[group_by].unique()) + j % len(color_scale)]
                    ))
        else:
            for i, y_column in enumerate(y_columns):
                if y_column in sorted_df.columns:
                    aggregated = sorted_df.groupby(x_column)[y_column].mean().nlargest(max_bars)
                    if orientation == 'v':
                        x_data = aggregated.index
                        y_data = aggregated.values
                    else:
                        x_data = aggregated.values
                        y_data = aggregated.index
                    
                    if self.sort_bars.isChecked():
                        sorted_indices = y_data.argsort()
                        x_data = x_data[sorted_indices]
                        y_data = y_data[sorted_indices]

                    fig.add_trace(go.Bar(
                        x=x_data,
                        y=y_data,
                        name=y_column,
                        orientation=orientation,
                        marker_color=color_scale[i % len(color_scale)]
                    ))

        # Update layout
        fig.update_layout(
            title=self.title_input.text() or f"Top {max_bars} {', '.join(y_columns)} by {x_column}",
            barmode=bar_mode,
            xaxis_title=x_column if orientation == 'v' else ', '.join(y_columns),
            yaxis_title=', '.join(y_columns) if orientation == 'v' else x_column,
            showlegend=self.show_legend.isChecked(),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        # Improve axis formatting
        if pd.api.types.is_numeric_dtype(sorted_df[x_column]):
            fig.update_xaxes(tickformat=",.0f")
        elif pd.api.types.is_datetime64_any_dtype(sorted_df[x_column]):
            fig.update_xaxes(tickformat="%Y-%m-%d")

        if not for_subplot:
            # Add range slider
            fig.update_xaxes(rangeslider_visible=True)

            # Enable zooming and panning
            fig.update_layout(dragmode='pan')
            config = {'scrollZoom': True, 'displayModeBar': True, 'editable': True}

            self.plot_view.setHtml(fig.to_html(include_plotlyjs='cdn', config=config))
        
        return fig

    def create_line_plot(self, x_column, y_columns, group_by=None, for_subplot=False):
        # Define a threshold for when to use resampler
        RESAMPLER_THRESHOLD = 10000  # Adjust this value as needed
        use_resampler = self.use_resampler and len(self.df) > RESAMPLER_THRESHOLD

        if use_resampler:
            fig = FigureResampler(go.Figure())
        else:
            fig = go.Figure()

        line_style = self.line_style.currentText()
        mode = "lines" if line_style == "Lines" else "lines+markers" if line_style == "Lines+Markers" else "markers"

        use_secondary_y = self.secondary_y.isChecked()
        # Sort the dataframe by x_column
        sorted_df = self.df.sort_values(by=x_column)
        
        x_data = sorted_df[x_column].astype(str) if pd.api.types.is_datetime64_any_dtype(sorted_df[x_column]) else sorted_df[x_column]

        if group_by and group_by != "None":
            for y_column in y_columns:
                for group in sorted_df[group_by].unique():
                    group_data = sorted_df[sorted_df[group_by] == group]
                    y_data = group_data[y_column]
                    if self.smoothing.isChecked():
                        y_data = self.apply_smoothing(y_data)
                    
                    trace = go.Scattergl(
                        x=group_data[x_column],
                        y=y_data,
                        mode=mode,
                        name=f"{y_column} - {group}",
                        yaxis='y2' if use_secondary_y and y_column == y_columns[1] else 'y'
                    )
                    
                    if use_resampler:
                        fig.add_trace(trace, hf_x=group_data[x_column], hf_y=y_data)
                    else:
                        fig.add_trace(trace)
        else:
            for y_column in y_columns:
                y_data = sorted_df[y_column]
                if self.smoothing.isChecked():
                    y_data = self.apply_smoothing(y_data)
                
                trace = go.Scattergl(
                    x=x_data,
                    y=y_data,
                    mode=mode,
                    name=y_column,
                    yaxis='y2' if use_secondary_y and y_column == y_columns[1] else 'y'
                )
                
                if use_resampler:
                    fig.add_trace(trace, hf_x=x_data, hf_y=y_data)
                else:
                    fig.add_trace(trace)

        fig.update_layout(
            title=self.title_input.text() or f"{', '.join(y_columns)} over {x_column}",
            xaxis=dict(title=x_column),
            yaxis=dict(title=y_columns[0]),
            yaxis2=dict(title=y_columns[1], overlaying='y', side='right') if use_secondary_y and len(y_columns) > 1 else None,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="x unified"
        )

        fig.update_traces(opacity=0.7)

        if pd.api.types.is_datetime64_any_dtype(sorted_df[x_column]):
            fig.update_xaxes(
                tickformat="%Y-%m-%d %H:%M:%S",
                tickangle=45,
                tickmode='auto',
                nticks=10
            )

        if not for_subplot:
            if use_resampler:
                self.plot_view.setHtml(fig.to_html(include_plotlyjs='cdn', config={'responsive': True}))
            else:
                self.plot_view.setHtml(fig.to_html(include_plotlyjs='cdn'))
        
        return fig
    
    def apply_smoothing(self, data, window=5):
        return data.rolling(window=window).mean()

    def show_error(self, message):
        self.status_bar.showMessage(f"Error: {message}", 5000)  # Show for 5 seconds
        self.status_bar.setStyleSheet("color: red;")

    def show_success(self, message):
        self.status_bar.showMessage(message, 5000)  # Show for 5 seconds
        self.status_bar.setStyleSheet("color: green;")

class DataLoader(QThread):
    progress = pyqtSignal(int)
    dataLoaded = pyqtSignal(pd.DataFrame)

    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path

    def run(self):
        try:
            file_extension = os.path.splitext(self.file_path)[1].lower()
            if file_extension == '.csv':
                self.load_csv()
            elif file_extension in ['.xlsx', '.xls']:
                self.load_excel()
            elif file_extension == '.shp':
                self.load_shapefile()
            else:
                raise ValueError(f"Unsupported file type: {file_extension}")
        except Exception as e:
            print(f"Error loading data: {e}")

    def load_csv(self):
        try:
            encoding = self.detect_encoding()
            
            # Try reading with semicolon first
            df = self.read_csv_with_delimiter(encoding, ';')
            
            # If semicolon doesn't work well, try comma
            if df is None or df.shape[1] < 2:
                df = self.read_csv_with_delimiter(encoding, ',')
            
            # If comma doesn't work, try tab
            if df is None or df.shape[1] < 2:
                df = self.read_csv_with_delimiter(encoding, '\t')
            
            if df is None:
                raise ValueError("Unable to properly read the CSV file with any common delimiter.")

            # Clean up column names
            df.columns = df.columns.str.strip()

            # Replace empty strings with NaN
            df = df.replace(r'^\s*$', np.nan, regex=True)

            self.dataLoaded.emit(df)
            self.progress.emit(100)
        except Exception as e:
            print(f"Error loading CSV: {e}")
            raise

    def read_csv_with_delimiter(self, encoding, delimiter):
        try:
            df = pd.read_csv(self.file_path, encoding=encoding, sep=delimiter, low_memory=False, quoting=csv.QUOTE_MINIMAL)
            return df
        except Exception as e:
            print(f"Error reading CSV with delimiter '{delimiter}': {e}")
            return None

    def detect_encoding(self):
        encodings = ['utf-8', 'latin-1', 'iso-8859-1']
        for encoding in encodings:
            try:
                with open(self.file_path, 'r', encoding=encoding) as file:
                    file.read()
                return encoding
            except UnicodeDecodeError:
                continue
        return 'utf-8'  # Default to UTF-8 if no encoding works

    # ... (other methods remain the same)

    def convert_decimal(self, s):
        try:
            return float(s.replace(',', '.'))
        except (ValueError, TypeError):
            return s
        
    def convert_columns(self, df):
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except ValueError:
                pass
        return df
    
    def load_shapefile(self):
        self.progress.emit(10)  # Initial progress update
        gdf = gpd.read_file(self.file_path)
        self.progress.emit(50)  # Midway progress update
        QThread.sleep(1)  # Simulate some delay
        df = pd.DataFrame(gdf.drop(columns='geometry'))  # Convert GeoDataFrame to DataFrame, drop geometry for simplicity
        self.dataLoaded.emit(df)
        self.progress.emit(100)  # Completion progress 

    def load_excel(self):
        try:
            self.progress.emit(10)  # Initial progress update
            QThread.sleep(1)  # Simulate some delay
            df = pd.read_excel(self.file_path)
           # df = self.convert_columns(df)  # Convert columns before emitting the signal
            self.progress.emit(50)  # Midway progress update
            QThread.sleep(1)  # Simulate some delay
            self.dataLoaded.emit(df)
            self.progress.emit(100)  # Completion progress update
        except Exception as e:
            print(f"Failed to load Excel file: {e}")

    def count_rows(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                return sum(1 for row in file)
        except UnicodeDecodeError:
            with open(self.file_path, 'r', encoding='latin-1') as file:
                return sum(1 for row in file)
         
class SQLDataLoader(QThread):
    progress = pyqtSignal(int)
    dataLoaded = pyqtSignal(pd.DataFrame)
    dataSize = pyqtSignal(int, int)

    def __init__(self, conn_str, table_name, date_column=None, start_date=None, end_date=None, selected_columns=None):
        super().__init__()
        self.conn_str = conn_str
        self.table_name = table_name
        self.date_column = date_column
        self.start_date = start_date
        self.end_date = end_date
        self.selected_columns = selected_columns or '*'
        self.primary_keys = self.fetch_primary_keys()

    def create_connection(self):
        """Creates a new database connection."""
        return pyodbc.connect(self.conn_str)

    def fetch_primary_keys(self):
        query = """
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_NAME = ? AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
        ORDER BY ORDINAL_POSITION
        """
        conn = self.create_connection()
        cursor = conn.cursor()
        cursor.execute(query, (self.table_name,))
        results = cursor.fetchall()
        conn.close()
        return [result[0] for result in results]

    def run(self):
        try:
            conn = self.create_connection()
            base_query = f"SELECT {self.selected_columns} FROM {self.table_name}"
            where_clauses = []
            params = []

            if self.date_column and self.start_date:
                where_clauses.append(f"{self.date_column} >= ?")
                params.append(self.start_date)

            if self.date_column and self.end_date:
                where_clauses.append(f"{self.date_column} <= ?")
                params.append(self.end_date)

            if where_clauses:
                base_query += " WHERE " + " AND ".join(where_clauses)

            cursor = conn.cursor()
            cursor.fast_executemany = True
            count_query = base_query.replace(f"SELECT {self.selected_columns}", "SELECT COUNT(*)")
            cursor.execute(count_query, params)
            total_rows = cursor.fetchone()[0]

            if total_rows == 0:
                self.dataSize.emit(0, 0)
                conn.close()
                return

            chunk_size = 75000
            df_list = []
            offset = 0
            iterations = 0

            if self.primary_keys:
                primary_key_order = ', '.join(self.primary_keys)
                while offset < total_rows:
                    chunk_query = f"{base_query} ORDER BY {primary_key_order} OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
                    df_chunk = pd.read_sql(chunk_query, conn, params=params)
                    if df_chunk.empty:
                        break
                   # df_chunk = self.convert_columns(df_chunk)  # Convert columns before emitting the signal
                    df_list.append(df_chunk)
                    offset += chunk_size
                    iterations += 1
                    progress_percent = int((iterations * chunk_size / total_rows) * 100)
                    self.progress.emit(min(progress_percent, 99))
            else:
                df = pd.read_sql(base_query, conn, params=params)
                #df = self.convert_columns(df)  # Convert columns before emitting the signal
                self.dataLoaded.emit(df)
                self.dataSize.emit(len(df), len(df.columns))
                self.progress.emit(100)
                conn.close()
                return

            df = pd.concat(df_list, axis=0)
            self.dataLoaded.emit(df)
            self.dataSize.emit(len(df), len(df.columns))
            self.progress.emit(100)
            conn.close()
        except Exception as e:
            print(f"Error loading data: {e}")
            self.dataSize.emit(0, 0)
            
    def convert_columns(self, df):
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except ValueError:
                pass
        return df
    
class DataSourceDialog(QDialog):
    def __init__(self, parent=None, initial_server='', initial_database=''):
        super().__init__(parent)
        self.setWindowTitle("Choose Data Source")

        # Radio buttons for data source selection
        self.localFileButton = QRadioButton("Load from Local File")
        self.sqlDatabaseButton = QRadioButton("Load from SQL Database")
        self.localFileButton.setChecked(True)  # Default to local file

        # Line edits for SQL connection details, initialized with provided values
        self.serverInput = QLineEdit(initial_server)
        self.databaseInput = QLineEdit(initial_database)

        # Layout for SQL connection details
        self.sqlFormLayout = QFormLayout()
        self.sqlFormLayout.addRow("Server:", self.serverInput)
        self.sqlFormLayout.addRow("Database:", self.databaseInput)

        # Button box for dialog actions
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttonBox.accepted.connect(self.accept)  # Correct signal connection
        self.buttonBox.rejected.connect(self.reject)  # Correct signal connection

        # Main layout
        layout = QVBoxLayout()
        layout.addWidget(self.localFileButton)
        layout.addWidget(self.sqlDatabaseButton)
        layout.addLayout(self.sqlFormLayout)
        layout.addWidget(self.buttonBox)
        self.setLayout(layout)

        # Connection details should only be visible if SQL database is selected
        self.sqlDatabaseButton.toggled.connect(self.toggleSQLInputFields)
        self.toggleSQLInputFields(self.sqlDatabaseButton.isChecked())

    def toggleSQLInputFields(self, checked):
        # Toggle the visibility of SQL input fields based on the selected option
        self.serverInput.setVisible(checked)
        self.databaseInput.setVisible(checked)
        
class DataFrameModel(QAbstractTableModel):
    def __init__(self, data=pd.DataFrame()):
        super().__init__()
        self._data = data
        self._display_cache = {}
        self._sort_column = -1
        self._sort_order = Qt.SortOrder.AscendingOrder
        
    def sort(self, column, order=Qt.SortOrder.AscendingOrder):
        self.layoutAboutToBeChanged.emit()
        self._sort_column = column
        self._sort_order = order
        self.sort_data()
        self.layoutChanged.emit()

    def sort_data(self):
        if self._sort_column >= 0 and self._sort_column < len(self._data.columns):
            col_name = self._data.columns[self._sort_column]
            try:
                ascending = self._sort_order == Qt.SortOrder.AscendingOrder
                if pd.api.types.is_numeric_dtype(self._data[col_name]):
                    # For numeric columns, use numeric sorting
                    self._data = self._data.sort_values(col_name, ascending=ascending, na_position='last')
                else:
                    # For non-numeric columns, use lexicographic sorting
                    self._data = self._data.sort_values(col_name, ascending=ascending, na_position='last', key=lambda x: x.astype(str).str.lower())
            except Exception as e:
                print(f"Error sorting column {col_name}: {e}")
            self._display_cache.clear()  # Clear cache after sorting
        elif self._sort_column != -1:
            print(f"Attempted to sort on invalid column index: {self._sort_column}")

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None

        row, col = index.row(), index.column()
        value = self._data.iat[row, col]

        if role == Qt.ItemDataRole.DisplayRole or role == Qt.ItemDataRole.EditRole:
            cache_key = (row, col)
            if cache_key in self._display_cache:
                return self._display_cache[cache_key]
            
            formatted_value = self.format_value(value)
            self._display_cache[cache_key] = formatted_value
            return formatted_value

        elif role == Qt.ItemDataRole.BackgroundRole:
            return QBrush(QColor("#f0f0f0")) if row % 2 == 0 else QBrush(Qt.GlobalColor.white)

        elif role == Qt.ItemDataRole.TextAlignmentRole:
            return self.get_alignment(value)

        elif role == Qt.ItemDataRole.ToolTipRole:
            return str(value)

        return None
    
    def dropColumns(self, columns_to_drop):
        if not self._data.empty:
            # Drop the columns safely
            self._data.drop(columns=columns_to_drop, inplace=True, errors='ignore')
            self.beginResetModel()  # Prepare the model for changes
            self._display_cache.clear()  # Clear the cache since the data structure has changed
            self.endResetModel()  # Reset the model to update views
    
    def format_value(self, value):
        if pd.isna(value):
            return ""
        elif isinstance(value, (int, np.integer)):
            return f"{value:,}"
        elif isinstance(value, (float, np.float64)):
            return f"{value:.2f}"
        elif isinstance(value, (pd.Timestamp, datetime)):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    def get_alignment(self, value):
        if isinstance(value, (int, float, np.integer, np.float64)):
            return Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        return Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter     
         
    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal:
            if role == Qt.ItemDataRole.DisplayRole and section < len(self._data.columns):
                return str(self._data.columns[section])
            elif role == Qt.ItemDataRole.FontRole:
                font = QFont()
                font.setBold(True)
                return font
            elif role == Qt.ItemDataRole.BackgroundRole:
                return QBrush(QColor("#e0e0e0"))
        elif orientation == Qt.Orientation.Vertical:
            if role == Qt.ItemDataRole.DisplayRole and section < self._data.shape[0]:
                return str(section + 1)
        return None

    def setData(self, index, value, role=Qt.ItemDataRole.EditRole):
        if not index.isValid() or role != Qt.ItemDataRole.EditRole:
            return False
        self._data.iat[index.row(), index.column()] = value
        self.dataChanged.emit(index, index, [role])
        self._display_cache.pop((index.row(), index.column()), None)  # Invalidate cache
        return True

    def flags(self, index):
        if not index.isValid():
            return Qt.ItemFlag.ItemIsEnabled
        return Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsEditable

    def clearData(self):
        self.beginResetModel()
        self._data = pd.DataFrame()
        self._display_cache.clear()
        self.endResetModel()

    def loadData(self, data):
        self.beginResetModel()
        self._data = data
        self._display_cache.clear()  # Clear the cache to prevent stale display data
        self.endResetModel()  # This will notify all views using this model to update based on new data
        
    def reorder_columns(self, new_order):
        self.layoutAboutToBeChanged.emit()
        self._data = self._data.reindex(columns=new_order)
        self._display_cache.clear()
        self.layoutChanged.emit()
class ColumnEditorDialog(QDialog):
    def __init__(self, columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Columns")
        self.columns = columns
        self.setup_ui()

    def setup_ui(self):
        layout = QHBoxLayout(self)

        # Column list
        self.column_list = QListWidget()
        self.column_list.addItems(self.columns)
        self.column_list.currentItemChanged.connect(self.on_selection_change)
        layout.addWidget(self.column_list)

        edit_layout = QVBoxLayout()

        # Column name edit
        self.name_edit = QLineEdit()
        edit_layout.addWidget(QLabel("Column Name:"))
        edit_layout.addWidget(self.name_edit)

        # Buttons
        self.rename_btn = QPushButton("Rename")
        self.rename_btn.clicked.connect(self.rename_column)
        edit_layout.addWidget(self.rename_btn)

        self.move_up_btn = QPushButton("Move Up")
        self.move_up_btn.clicked.connect(self.move_up)
        edit_layout.addWidget(self.move_up_btn)

        self.move_down_btn = QPushButton("Move Down")
        self.move_down_btn.clicked.connect(self.move_down)
        edit_layout.addWidget(self.move_down_btn)

        layout.addLayout(edit_layout)

        # OK and Cancel buttons
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def on_selection_change(self, current, previous):
        if current:
            self.name_edit.setText(current.text())

    def rename_column(self):
        current_item = self.column_list.currentItem()
        if current_item:
            new_name = self.name_edit.text()
            current_item.setText(new_name)

    def move_up(self):
        current_row = self.column_list.currentRow()
        if current_row > 0:
            item = self.column_list.takeItem(current_row)
            self.column_list.insertItem(current_row - 1, item)
            self.column_list.setCurrentItem(item)

    def move_down(self):
        current_row = self.column_list.currentRow()
        if current_row < self.column_list.count() - 1:
            item = self.column_list.takeItem(current_row)
            self.column_list.insertItem(current_row + 1, item)
            self.column_list.setCurrentItem(item)

    def get_updated_columns(self):
        return [self.column_list.item(i).text() for i in range(self.column_list.count())]


from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QGroupBox, 
                             QComboBox, QLineEdit, QListWidget, QTreeWidget, 
                             QTreeWidgetItem, QPushButton, QLabel, QSlider, 
                             QAbstractItemView, QWidget)
from PyQt6.QtCore import Qt, pyqtSignal

class FilterDialog(QDialog):
    filtersCleared = pyqtSignal(str)  # The str parameter is for the dataset name
    
    def __init__(self, dataframes, parent=None, current_filters=None):
        super().__init__(parent)
        self.setWindowTitle('Filter Data')
        self.setMinimumSize(800, 600)
        self.dataframes = dataframes
        self.current_filters = current_filters if current_filters is not None else {}
        self.selected_dataset = list(dataframes.keys())[0]
        self.filters = self.current_filters.get(self.selected_dataset, {}).copy()
        self.input_widgets = {}
        self.search_bars = {}

        self.setup_ui()

    def setup_ui(self):
        main_layout = QHBoxLayout(self)
        
        # Left panel for filters
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        main_layout.addWidget(left_panel, 1)

        # Dataset selector
        dataset_group = QGroupBox("Dataset Selection")
        dataset_layout = QVBoxLayout(dataset_group)
        self.dataset_selector = QComboBox()
        self.dataset_selector.addItems(list(self.dataframes.keys()))
        self.dataset_selector.currentIndexChanged.connect(self.update_selected_dataset)
        dataset_layout.addWidget(self.dataset_selector)
        left_layout.addWidget(dataset_group)

        # Column selection
        column_group = QGroupBox("Column Selection")
        column_layout = QVBoxLayout(column_group)
        self.column_search = QLineEdit()
        self.column_search.setPlaceholderText("Search columns...")
        self.column_search.textChanged.connect(self.filter_columns)
        column_layout.addWidget(self.column_search)
        self.column_select = QListWidget()
        self.column_select.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        self.column_select.itemSelectionChanged.connect(self.update_input_widgets)
        column_layout.addWidget(self.column_select)
        left_layout.addWidget(column_group)

        # Cross-reference options
        cross_ref_group = QGroupBox("Cross-Reference")
        cross_ref_layout = QVBoxLayout(cross_ref_group)
        self.cross_reference_checkbox = QCheckBox("Enable Cross-Reference")
        self.cross_reference_checkbox.stateChanged.connect(self.toggle_cross_reference)
        cross_ref_layout.addWidget(self.cross_reference_checkbox)
        self.cross_reference_dataset_select = QComboBox()
        self.cross_reference_column_select = QComboBox()
        cross_ref_layout.addWidget(self.cross_reference_dataset_select)
        cross_ref_layout.addWidget(self.cross_reference_column_select)
        left_layout.addWidget(cross_ref_group)

        # Right panel
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        main_layout.addWidget(right_panel, 2)

        # Filter input area
        self.filter_group = QGroupBox("Filter Input")
        self.filter_layout = QVBoxLayout(self.filter_group)
        right_layout.addWidget(self.filter_group)

        # Set the filter group to expand and fill available space
        size_policy = QSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.filter_group.setSizePolicy(size_policy)

        # Active filters
        active_filters_group = QGroupBox("Active Filters")
        active_filters_layout = QVBoxLayout(active_filters_group)
        self.active_filters_tree = QTreeWidget()
        self.active_filters_tree.setHeaderLabels(["Column", "Filter"])
        active_filters_layout.addWidget(self.active_filters_tree)
        right_layout.addWidget(active_filters_group)

        
        # Buttons
        button_layout = QHBoxLayout()
        self.apply_button = QPushButton('Apply Filters')
        self.apply_button.clicked.connect(self.apply_filters_filterdialog)
        self.clear_button = QPushButton('Clear Filters')
        self.clear_button.clicked.connect(self.clear_filters)
        self.cancel_button = QPushButton('Cancel')
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.apply_button)
        button_layout.addWidget(self.clear_button)
        button_layout.addWidget(self.cancel_button)
        right_layout.addLayout(button_layout)

        self.reset_dialog()
                    
    def reset_dialog(self):
        self.clear_layout(self.filter_layout)
        self.update_column_selector()
        self.update_active_filters_display_filterdialog()
        self.cross_reference_checkbox.setChecked(False)
        self.toggle_cross_reference(False)
        self.column_select.clearSelection()
        
    def clear_layout(self, layout):
        while layout.count():
            child = layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
            elif child.layout():
                self.clear_layout(child.layout())
                
    def clear_filter_input_area(self):
        while self.filter_layout.count():
            item = self.filter_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
                
    def update_column_selector(self):
        self.column_select.clear()
        if self.selected_dataset in self.dataframes:
            self.column_select.addItems(self.dataframes[self.selected_dataset].columns.tolist())  
                
    def filter_columns(self, text):
        for i in range(self.column_select.count()):
            item = self.column_select.item(i)
            item.setHidden(text.lower() not in item.text().lower())
                    
    def update_selected_dataset(self):
        self.selected_dataset = self.dataset_select.currentText()
        dataframe = self.dataframes[self.selected_dataset]
        self.filters = self.current_filters.get(self.selected_dataset, {}).copy()
        self.column_select.clear()
        self.column_select.addItems(dataframe.columns.tolist())
        self.reset_dialog()  # Reset the dialog when changing datasets
        self.update_cross_reference_options()

    def filter_list_items(self, text, list_widget):
        for i in range(list_widget.count()):
            item = list_widget.item(i)
            item.setHidden(text.lower() not in item.text().lower())

    def toggle_cross_reference(self, checked):
        self.cross_reference_dataset_select.setEnabled(checked)
        self.cross_reference_column_select.setEnabled(checked)

    def update_cross_reference_options(self):
        other_datasets = [name for name in self.dataframes if name != self.selected_dataset]
        self.cross_reference_dataset_select.clear()
        self.cross_reference_dataset_select.addItems(other_datasets)
        if other_datasets:
            self.cross_reference_dataset_select.setCurrentIndex(0)
            self.update_cross_reference_columns()

    def update_cross_reference_columns(self):
        other_dataset = self.cross_reference_dataset_select.currentText()
        if other_dataset:
            columns = self.dataframes[other_dataset].columns.tolist()
            self.cross_reference_column_select.clear()
            self.cross_reference_column_select.addItems(columns)

    def create_filter_widget(self, column, dataframe):
        group_box = QGroupBox(column)
        group_box.setFixedSize(450, 350)  # Set a fixed size for each filter widget
        
        main_layout = QVBoxLayout(group_box)
        
        if pd.api.types.is_numeric_dtype(dataframe[column]):
            # Numeric column handling (sliders)
            min_val, max_val = self.get_numeric_range(dataframe[column])
            min_slider = QSlider(Qt.Orientation.Horizontal)
            max_slider = QSlider(Qt.Orientation.Horizontal)
            
            min_slider.setRange(min_val, max_val)
            max_slider.setRange(min_val, max_val)
            
            min_slider.setValue(min_val)
            max_slider.setValue(max_val)
            
            range_label = QLabel(f"Range: {min_val} - {max_val}")
            
            main_layout.addWidget(QLabel("Min:"))
            main_layout.addWidget(min_slider)
            main_layout.addWidget(QLabel("Max:"))
            main_layout.addWidget(max_slider)
            main_layout.addWidget(range_label)
            
            def update_range():
                min_v = min(min_slider.value(), max_slider.value())
                max_v = max(min_slider.value(), max_slider.value())
                range_label.setText(f"Range: {min_v} - {max_v}")
            
            min_slider.valueChanged.connect(update_range)
            max_slider.valueChanged.connect(update_range)
        else:
            # Non-numeric column handling (list widget, search, and regex)
            unique_values = dataframe[column].dropna().unique()
            list_widget = QListWidget()
            list_widget.addItems([str(val) for val in unique_values])
            list_widget.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
            main_layout.addWidget(QLabel("Select values:"))
            main_layout.addWidget(list_widget)

            search_bar = QLineEdit()
            search_bar.setPlaceholderText(f"Search {column}...")
            search_bar.textChanged.connect(lambda text, lw=list_widget: self.filter_list_items(text, lw))
            main_layout.addWidget(search_bar)

            regex_input = QLineEdit()
            regex_input.setPlaceholderText("Enter regex pattern...")
            main_layout.addWidget(QLabel("Regex filter:"))
            main_layout.addWidget(regex_input)

        main_layout.addStretch(1)
        
        return group_box

    def update_input_widgets(self):
        self.clear_filter_input_area()
        selected_columns = [item.text() for item in self.column_select.selectedItems()]
        dataframe = self.dataframes[self.selected_dataset]

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)

        for column in selected_columns:
            filter_widget = self.create_filter_widget(column, dataframe)
            scroll_layout.addWidget(filter_widget)

        scroll_layout.addStretch(1)
        scroll_content.setLayout(scroll_layout)
        scroll_area.setWidget(scroll_content)

        self.filter_layout.addWidget(scroll_area)

    def get_numeric_range(self, series):
        min_val = series.min()
        max_val = series.max()
        
        if pd.isna(min_val) or pd.isna(max_val):
            return 0, 100
        
        min_val = max(int(min_val), -2147483648)
        max_val = min(int(max_val), 2147483647)
        
        if min_val == max_val:
            max_val += 1
        
        return min_val, max_val

    def apply_filters_filterdialog(self, reapply=False):
        if self.dataframes[self.selected_dataset] is None:
            QMessageBox.warning(self, "No Data", "No dataset is loaded.")
            return

        dataframe = self.dataframes[self.selected_dataset]
        new_filters = {}
        
        if not reapply:
            scroll_area = self.filter_layout.itemAt(0).widget() if self.filter_layout.count() > 0 else None
            if not isinstance(scroll_area, QScrollArea):
                QMessageBox.warning(self, "Layout Error", "Expected scroll area not found.")
                return

            scroll_content = scroll_area.widget()
            scroll_layout = scroll_content.layout()

            for i in range(scroll_layout.count()):
                item = scroll_layout.itemAt(i)
                if item is None or item.widget() is None:
                    continue

                group_box = item.widget()
                if not isinstance(group_box, QGroupBox):
                    continue

                column = group_box.title()
                layout = group_box.layout()

                if pd.api.types.is_numeric_dtype(dataframe[column]):
                    min_slider = layout.itemAt(1).widget()
                    max_slider = layout.itemAt(3).widget()
                    min_val = min(min_slider.value(), max_slider.value())
                    max_val = max(min_slider.value(), max_slider.value())
                    column_min = dataframe[column].min()
                    column_max = dataframe[column].max()
                    if pd.notna(column_min) and pd.notna(column_max):
                        if min_val != column_min or max_val != column_max:
                            new_filters[column] = {'range': (min_val, max_val)}
                else:
                    list_widget = layout.itemAt(1).widget()
                    selected_items = [item.text() for item in list_widget.selectedItems()]
                    regex_input = layout.itemAt(4).widget()
                    regex_pattern = regex_input.text().strip()

                    if selected_items or regex_pattern:
                        new_filters[column] = {}
                        if selected_items:
                            new_filters[column]['in'] = selected_items
                        if regex_pattern:
                            new_filters[column]['regex'] = regex_pattern

            if self.cross_reference_checkbox.isChecked():
                cross_reference_dataset = self.cross_reference_dataset_select.currentText()
                cross_reference_column = self.cross_reference_column_select.currentText()
                if cross_reference_dataset and cross_reference_column:
                    new_filters['cross_reference'] = {
                        'dataset': cross_reference_dataset,
                        'column': cross_reference_column
                    }

            # Update filters with new_filters, preserving existing filters
            self.filters.update(new_filters)
        else:
            # When reapplying, use existing filters
            new_filters = self.filters

        # Apply the filters to the dataframe
        filtered_df = dataframe.copy()
        for column, filter_value in self.filters.items():
            if column == 'cross_reference':
                continue  # Skip cross-reference filter for now
            if 'range' in filter_value:
                min_val, max_val = filter_value['range']
                filtered_df = filtered_df[(filtered_df[column] >= min_val) & (filtered_df[column] <= max_val)]
            elif 'in' in filter_value:
                filtered_df = filtered_df[filtered_df[column].isin(filter_value['in'])]
            elif 'regex' in filter_value:
                filtered_df = filtered_df[filtered_df[column].str.contains(filter_value['regex'], na=False, regex=True)]

        # Apply cross-reference filter if present
        if 'cross_reference' in self.filters:
            cross_ref = self.filters['cross_reference']
            other_df = self.dataframes[cross_ref['dataset']]
            other_column = cross_ref['column']
            filtered_df = filtered_df[filtered_df[other_column].isin(other_df[other_column])]

        # Update the filtered dataframe and filters
        self.filtered_dataframe = filtered_df
        self.current_filters[self.selected_dataset] = self.filters

        # Update the UI
        self.update_active_filters_display_filterdialog()

        QMessageBox.information(self, "Filters Applied", 
                                f"Filters applied. Resulting dataset has {len(filtered_df)} rows.")
        if not reapply:
            self.accept()

    # ... existing code ...
                
    def update_active_filters_display_filterdialog(self):
        self.active_filters_tree.clear()
        for column, filter_value in self.filters.items():
            item = QTreeWidgetItem(self.active_filters_tree)
            item.setText(0, column)
            if isinstance(filter_value, dict):
                if 'range' in filter_value:
                    item.setText(1, f"Range: {filter_value['range'][0]} - {filter_value['range'][1]}")
                elif 'in' in filter_value:
                    item.setText(1, f"In: {', '.join(filter_value['in'])}")
                elif 'regex' in filter_value:
                    item.setText(1, f"Regex: {filter_value['regex']}")
            else:
                item.setText(1, str(filter_value))

    def clear_filters(self):
        self.filters = {}
        self.current_filters[self.selected_dataset] = {}
        self.filtered_dataframe = self.dataframes[self.selected_dataset].copy()
        self.update_active_filters_display_filterdialog()
        self.reset_dialog()
        self.filtersCleared.emit(self.selected_dataset)
        
    def get_filters(self):
        return self.current_filters
                
class GroupAndAggregateDialog(QDialog):
    def __init__(self, parent, columns):
        super().__init__(parent)
        self.setWindowTitle("Group and Aggregate Data")
        layout = QVBoxLayout(self)

        # Group columns selection
        self.group_columns_selector = QListWidget()
        self.group_columns_selector.addItems(columns)
        self.group_columns_selector.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        layout.addWidget(QLabel("Select columns to group by:"))
        layout.addWidget(self.group_columns_selector)

        # Value column selection
        self.value_column_selector = QComboBox()
        self.value_column_selector.addItems(columns)
        layout.addWidget(QLabel("Select value column for aggregation:"))
        layout.addWidget(self.value_column_selector)

        # Aggregation functions selection
        self.agg_funcs_selector = QListWidget()
        self.agg_funcs_selector.addItems(["mean", "median", "sum", "min", "max", "count"])
        self.agg_funcs_selector.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        layout.addWidget(QLabel("Select aggregation function(s):"))
        layout.addWidget(self.agg_funcs_selector)

        # Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_selections(self):
        group_columns = [item.text() for item in self.group_columns_selector.selectedItems()]
        value_column = self.value_column_selector.currentText()
        agg_funcs = [item.text() for item in self.agg_funcs_selector.selectedItems()]
        return group_columns, value_column, agg_funcs


class DataAnalytics(QWidget):
    dataUpdated = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.dataframe = None
        self.statistics = {}
        self.setup_ui()

    def setup_ui(self):
        main_layout = QHBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(splitter)

        # Left side: Table and search
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search columns...")
        self.search_box.textChanged.connect(self.filter_table)
        left_layout.addWidget(self.search_box)
        self.stats_table_widget = QTableWidget()
        self.stats_table_widget.setSortingEnabled(True)
        self.stats_table_widget.itemSelectionChanged.connect(self.update_column_details)
        left_layout.addWidget(self.stats_table_widget)
        splitter.addWidget(left_widget)

        # Right side: Stacked groups
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        splitter.addWidget(right_widget)

        # Upper group: Overall Statistics and Column Details
        upper_group = QGroupBox("Statistics")
        upper_layout = QVBoxLayout(upper_group)

        # Create a splitter for overall stats and column details
        stats_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Overall Statistics
        overall_stats_scroll = QScrollArea()
        overall_stats_scroll.setWidgetResizable(True)
        overall_stats_content = QWidget()
        overall_stats_layout = QVBoxLayout(overall_stats_content)
        self.overall_stats_label = QLabel()
        overall_stats_layout.addWidget(self.overall_stats_label)
        overall_stats_scroll.setWidget(overall_stats_content)
        stats_splitter.addWidget(overall_stats_scroll)

        # Column Details
        column_details_scroll = QScrollArea()
        column_details_scroll.setWidgetResizable(True)
        column_details_content = QWidget()
        column_details_layout = QVBoxLayout(column_details_content)
        self.column_details_label = QLabel()
        column_details_layout.addWidget(self.column_details_label)
        column_details_scroll.setWidget(column_details_content)
        stats_splitter.addWidget(column_details_scroll)

        upper_layout.addWidget(stats_splitter)
        right_layout.addWidget(upper_group)

        # Set the size of the upper group
        upper_group.setMaximumHeight(300)  # Adjust this value as needed

        # Lower group: Visualizations
        lower_group = QGroupBox("Visualizations")
        lower_layout = QVBoxLayout(lower_group)
        self.plot_selector = QComboBox()
        self.plot_selector.addItems(["Missing Value Counts", "Data Type Distribution"])
        self.plot_selector.currentIndexChanged.connect(self.update_visualization)
        lower_layout.addWidget(self.plot_selector)
        self.plot_view = QWebEngineView()
        lower_layout.addWidget(self.plot_view)
        right_layout.addWidget(lower_group)

        # Set the initial sizes of the splitter
        splitter.setSizes([int(self.width() * 0.4), int(self.width() * 0.6)])

        self.setLayout(main_layout)

    def set_dataframe(self, dataframe):
        self.dataframe = dataframe
        self.handle_missing_data()
        # Remove the automatic statistics calculation
        # self.update_statistics()
        self.dataUpdated.emit()

    def handle_missing_data(self):
        object_columns = self.dataframe.select_dtypes(include=['object'])
        for col in object_columns.columns:
            self.dataframe[col] = self.dataframe[col].apply(lambda x: None if x == '' else x)

    def compute_statistics(self):
        if not self.dataframe.empty:
            self.statistics = {
                'row_count': self.dataframe.shape[0],
                'column_count': self.dataframe.shape[1],
                'columns': {}
            }

            for column in self.dataframe.columns:
                mode_values = self.dataframe[column].mode()
                mode_freq = self.dataframe[column].value_counts().iloc[0] if not mode_values.empty else None
                mode_value = mode_values.iloc[0] if not mode_values.empty else None
                column_stats = {
                    'data_type': str(self.dataframe[column].dtype),
                    'missing_count': self.dataframe[column].isnull().sum(),
                    'unique_count': self.dataframe[column].nunique(),
                    'mode': mode_value,
                    'mode_frequency': mode_freq
                }

                if pd.api.types.is_numeric_dtype(self.dataframe[column]):
                    column_stats.update({
                        'mean': self.dataframe[column].mean(),
                        'median': self.dataframe[column].median(),
                        '25th_percentile': self.dataframe[column].quantile(0.25),
                        '50th_percentile': self.dataframe[column].quantile(0.50),
                        '75th_percentile': self.dataframe[column].quantile(0.75),
                        'min': self.dataframe[column].min(),
                        'max': self.dataframe[column].max(),
                        'variance': self.dataframe[column].var(),
                        'std_dev': self.dataframe[column].std(),
                        'skewness': self.dataframe[column].skew(),
                        'kurtosis': self.dataframe[column].kurt()
                    })

                self.statistics['columns'][column] = column_stats
        else:
            self.statistics = None

    def update_statistics(self):
        self.compute_statistics()
        if self.statistics:
            self.populate_stats_table()
            self.show_overall_statistics()
            self.update_visualization()

    def populate_stats_table(self):
        columns = ['Column Name', 'Data Type', 'Missing Values', 'Unique Values', 'Mode', 'Mode Frequency']
        self.stats_table_widget.setColumnCount(len(columns))
        self.stats_table_widget.setHorizontalHeaderLabels(columns)

        self.stats_table_widget.setRowCount(len(self.statistics['columns']))
        for row, (column, stats) in enumerate(self.statistics['columns'].items()):
            self.stats_table_widget.setItem(row, 0, QTableWidgetItem(column))
            self.stats_table_widget.setItem(row, 1, QTableWidgetItem(stats['data_type']))
            self.stats_table_widget.setItem(row, 2, QTableWidgetItem(str(stats['missing_count'])))
            self.stats_table_widget.setItem(row, 3, QTableWidgetItem(str(stats['unique_count'])))
            self.stats_table_widget.setItem(row, 4, QTableWidgetItem(str(stats['mode'])))
            self.stats_table_widget.setItem(row, 5, QTableWidgetItem(str(stats['mode_frequency'])))

        self.stats_table_widget.resizeColumnsToContents()
        self.stats_table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

    def filter_table(self, text):
        for row in range(self.stats_table_widget.rowCount()):
            match = False
            for column in range(self.stats_table_widget.columnCount()):
                item = self.stats_table_widget.item(row, column)
                if item and text.lower() in item.text().lower():
                    match = True
                    break
            self.stats_table_widget.setRowHidden(row, not match)

    def show_overall_statistics(self):
        if not self.statistics:
            self.overall_stats_label.setText("No data available to show overall statistics.")
            return

        total_missing = sum(stats['missing_count'] for stats in self.statistics['columns'].values())
        avg_missing_percentage = total_missing / (self.statistics['row_count'] * self.statistics['column_count']) * 100
        duplicate_rows = self.dataframe.duplicated().sum()
        memory_usage = self.dataframe.memory_usage(deep=True).sum() / 1024 ** 2  # Convert to MB

        stats_html = f"""
        <style>
            .stats-container {{
                font-family: Arial, sans-serif;
                background-color: #f5f5f5;
                border-radius: 10px;
                padding: 20px;
                max-width: 400px;
            }}
            .stat-item {{
                display: flex;
                justify-content: space-between;
                margin-bottom: 10px;
            }}
            .stat-label {{
                font-weight: bold;
                color: #333;
            }}
            .stat-value {{
                color: #0066cc;
            }}
        </style>
        <div class="stats-container">
            <h2 style="text-align: center; color: #333;">Overall Statistics</h2>
            <div class="stat-item">
                <span class="stat-label">Row Count:</span>
                <span class="stat-value">{self.statistics['row_count']:,}</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Column Count:</span>
                <span class="stat-value">{self.statistics['column_count']}</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Total Missing Values:</span>
                <span class="stat-value">{total_missing:,}</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Avg Missing Percentage:</span>
                <span class="stat-value">{avg_missing_percentage:.2f}%</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Duplicate Rows:</span>
                <span class="stat-value">{duplicate_rows:,}</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Memory Usage:</span>
                <span class="stat-value">{memory_usage:.2f} MB</span>
            </div>
        </div>
        """
        self.overall_stats_label.setText(stats_html)
        self.overall_stats_label.setTextFormat(Qt.TextFormat.RichText)

    def update_visualization(self):
        selected_plot = self.plot_selector.currentText()
        if selected_plot == "Missing Value Counts":
            self.show_missing_value_counts()
        elif selected_plot == "Data Type Distribution":
            self.show_data_type_distribution()
    def show_missing_value_counts(self):
        missing_counts = [stats['missing_count'] for stats in self.statistics['columns'].values()]
        column_names = list(self.statistics['columns'].keys())
        fig = go.Figure(data=[go.Bar(x=column_names, y=missing_counts)])
        fig.update_layout(
            title="Missing Value Counts per Column",
            xaxis_title="Column",
            yaxis_title="Missing Value Count",
            height=400
        )
        self.plot_view.setHtml(fig.to_html(full_html=False, include_plotlyjs='cdn'))

    def show_data_type_distribution(self):
        data_types = [stats['data_type'] for stats in self.statistics['columns'].values()]
        type_counts = Counter(data_types)
        fig = go.Figure(data=[go.Pie(labels=list(type_counts.keys()), values=list(type_counts.values()))])
        fig.update_layout(
            title="Data Type Distribution",
            height=400
        )
        self.plot_view.setHtml(fig.to_html(full_html=False, include_plotlyjs='cdn'))
        
    def update_column_details(self):
        selected_items = self.stats_table_widget.selectedItems()
        if not selected_items:
            return
        
        column_name = self.stats_table_widget.item(selected_items[0].row(), 0).text()
        stats = self.statistics['columns'][column_name]
        
        details_html = f"""
        <style>
            .details-container {{
                font-family: Arial, sans-serif;
                background-color: #f5f5f5;
                border-radius: 10px;
                padding: 20px;
                max-width: 400px;
            }}
            .detail-item {{
                display: flex;
                justify-content: space-between;
                margin-bottom: 10px;
            }}
            .detail-label {{
                font-weight: bold;
                color: #333;
            }}
            .detail-value {{
                color: #0066cc;
            }}
        </style>
        <div class="details-container">
            <h2 style="text-align: center; color: #333;">Column Details: {column_name}</h2>
            <div class="detail-item">
                <span class="detail-label">Data Type:</span>
                <span class="detail-value">{stats['data_type']}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">Missing Values:</span>
                <span class="detail-value">{stats['missing_count']:,}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">Unique Values:</span>
                <span class="detail-value">{stats['unique_count']:,}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">Mode:</span>
                <span class="detail-value">{stats['mode']}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">Mode Frequency:</span>
                <span class="detail-value">{stats['mode_frequency']}</span>
            </div>
        """
        
        if 'mean' in stats:
            details_html += f"""
            <div class="detail-item">
                <span class="detail-label">Mean:</span>
                <span class="detail-value">{stats['mean']:.2f}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">Median:</span>
                <span class="detail-value">{stats['median']:.2f}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">25th Percentile:</span>
                <span class="detail-value">{stats['25th_percentile']:.2f}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">75th Percentile:</span>
                <span class="detail-value">{stats['75th_percentile']:.2f}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">Min:</span>
                <span class="detail-value">{stats['min']:.2f}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">Max:</span>
                <span class="detail-value">{stats['max']:.2f}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">Standard Deviation:</span>
                <span class="detail-value">{stats['std_dev']:.2f}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">Skewness:</span>
                <span class="detail-value">{stats['skewness']:.2f}</span>
            </div>
            <div class="detail-item">
                <span class="detail-label">Kurtosis:</span>
                <span class="detail-value">{stats['kurtosis']:.2f}</span>
            </div>
            """
        
        details_html += "</div>"
        self.column_details_label.setText(details_html)
        self.column_details_label.setTextFormat(Qt.TextFormat.RichText)

    def get_value_counts(self, column, limit=10):
        return self.dataframe[column].value_counts().nlargest(limit).to_dict()

class AdvancedAnalyticsView(QWidget):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent
        self.dataframe = None
        self.filtered_dataframe = None
        self.filters = {}
        self.nlargest_value = 10  # Default value for nlargest
        self.setup_ui()
            
    def setup_ui(self):
        main_layout = QHBoxLayout(self)
        
        # Left panel for filters
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        main_layout.addWidget(left_panel, 1)

        # Dataset selector
        dataset_group = QGroupBox("Dataset Selection")
        dataset_layout = QVBoxLayout(dataset_group)
        self.dataset_selector = QComboBox()
        self.dataset_selector.addItems(["Full Dataset", "Filtered Dataset"])
        self.dataset_selector.currentIndexChanged.connect(self.update_dataset)
        dataset_layout.addWidget(self.dataset_selector)
        left_layout.addWidget(dataset_group)

        # Filtering group
        filter_group = QGroupBox("Filtering")
        filter_layout = QVBoxLayout(filter_group)
        
        self.filter_column_selector = QComboBox()
        self.filter_column_selector.currentIndexChanged.connect(self.update_filter_values)
        filter_layout.addWidget(QLabel("Select Column for Filtering:"))
        filter_layout.addWidget(self.filter_column_selector)

        self.filter_search = QLineEdit()
        self.filter_search.setPlaceholderText("Search filter values...")
        self.filter_search.textChanged.connect(self.filter_values)
        filter_layout.addWidget(self.filter_search)

        self.filter_value_stack = QStackedWidget()
        filter_layout.addWidget(self.filter_value_stack)

        self.filter_list = QListWidget()
        self.filter_list.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        self.filter_value_stack.addWidget(self.filter_list)

        numeric_widget = QWidget()
        numeric_layout = QFormLayout(numeric_widget)
        self.numeric_operator = QComboBox()
        self.numeric_operator.addItems(['>', '<', '>=', '<=', '==', '!=', 'between'])
        self.numeric_operator.currentIndexChanged.connect(self.toggle_between_inputs)
        self.numeric_value = QLineEdit()
        self.numeric_value_2 = QLineEdit()
        numeric_layout.addRow("Operator:", self.numeric_operator)
        numeric_layout.addRow("Value:", self.numeric_value)
        numeric_layout.addRow("Value 2:", self.numeric_value_2)
        self.filter_value_stack.addWidget(numeric_widget)

        filter_buttons_layout = QHBoxLayout()
        self.add_filter_btn = QPushButton("Add Filter")
        self.add_filter_btn.clicked.connect(self.add_filter)
        self.remove_filters_btn = QPushButton("Remove Selected Filters")
        self.remove_filters_btn.clicked.connect(self.remove_selected_filters)
        filter_buttons_layout.addWidget(self.add_filter_btn)
        filter_buttons_layout.addWidget(self.remove_filters_btn)
        filter_layout.addLayout(filter_buttons_layout)

        left_layout.addWidget(filter_group)

        # Active filters group
        active_filters_group = QGroupBox("Active Filters")
        active_filters_layout = QVBoxLayout(active_filters_group)
        self.active_filters_tree = QTreeWidget()
        self.active_filters_tree.setHeaderLabels(["Column", "Values"])
        self.active_filters_tree.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        active_filters_layout.addWidget(self.active_filters_tree)
        left_layout.addWidget(active_filters_group)

        self.apply_filters_btn = QPushButton("Apply Filters")
        self.apply_filters_btn.clicked.connect(self.apply_filters)
        left_layout.addWidget(self.apply_filters_btn)

        self.row_count_label = QLabel()
        left_layout.addWidget(self.row_count_label)

        # Right panel for analysis
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        main_layout.addWidget(right_panel, 2)

        # Analysis group
        analysis_group = QGroupBox("Analysis")
        analysis_layout = QVBoxLayout(analysis_group)

        self.analysis_type = QComboBox()
        self.analysis_type.addItems(["", "Value Counts", "Simplified Value Counts", "Descriptive Statistics", "Distribution Plot"])
        self.analysis_type.currentIndexChanged.connect(self.update_analysis_options)
        analysis_layout.addWidget(QLabel("Select Analysis Type:"))
        analysis_layout.addWidget(self.analysis_type)

        self.column_selector = QComboBox()
        self.column_selector.currentIndexChanged.connect(self.update_analysis)
        analysis_layout.addWidget(QLabel("Select Column for Analysis:"))
        analysis_layout.addWidget(self.column_selector)

        self.nlargest_widget = QWidget()
        nlargest_layout = QHBoxLayout(self.nlargest_widget)
        nlargest_layout.addWidget(QLabel("Top N values:"))
        self.nlargest_input = QSpinBox()
        self.nlargest_input.setRange(1, 100)
        self.nlargest_input.setValue(self.nlargest_value)
        self.nlargest_input.valueChanged.connect(self.update_nlargest)
        nlargest_layout.addWidget(self.nlargest_input)
        analysis_layout.addWidget(self.nlargest_widget)
        self.nlargest_widget.hide()

        right_layout.addWidget(analysis_group)

        # Results area
        results_group = QGroupBox("Results")
        results_layout = QVBoxLayout(results_group)
        self.results_area = QTextEdit()
        self.results_area.setReadOnly(True)
        results_layout.addWidget(self.results_area)
        right_layout.addWidget(results_group)

        # Plot area
        plot_group = QGroupBox("Plot")
        plot_layout = QVBoxLayout(plot_group)
        self.plot_view = QWebEngineView()
        plot_layout.addWidget(self.plot_view)
        right_layout.addWidget(plot_group)
        plot_group.hide()

    def set_dataframe(self, df):
        self.dataframe = df
        self.filtered_dataframe = df.copy()
        self.update_column_selector()  # Update this first
        self.update_filter_values()  # Then update filter values
        self.update_analysis()
        self.update_row_count_label()  # Add this line
        
    def update_filter_values(self):
        column = self.filter_column_selector.currentText()
        if not column:  # Check if the column is an empty string
            return  # Exit the method if no column is selected

        if pd.api.types.is_numeric_dtype(self.dataframe[column]):
            self.filter_value_stack.setCurrentIndex(1)
            self.numeric_operator.currentIndexChanged.connect(self.toggle_between_inputs)
        else:
            self.filter_value_stack.setCurrentIndex(0)
            unique_values = self.dataframe[column].dropna().unique()
            self.filter_list.clear()
            self.filter_list.addItems([str(value) for value in unique_values])

    def update_column_selector(self):
        self.filter_column_selector.clear()
        self.column_selector.clear()
        if self.dataframe is not None and not self.dataframe.empty:
            columns = self.dataframe.columns.tolist()
            self.filter_column_selector.addItems(columns)
            self.column_selector.addItems(columns)
            # Set the current index to the first item if available
            if columns:
                self.filter_column_selector.setCurrentIndex(0)
                self.column_selector.setCurrentIndex(0)

    def update_dataset_selector(self):
        current_text = self.dataset_selector.currentText()
        self.dataset_selector.clear()
        self.dataset_selector.addItems(["Full Dataset", "Filtered Dataset"])
        if current_text in ["Full Dataset", "Filtered Dataset"]:
            self.dataset_selector.setCurrentText(current_text)
        else:
            self.dataset_selector.setCurrentText("Full Dataset")
    def update_dataset(self):
        if self.dataset_selector.currentText() == "Full Dataset":
            self.filtered_dataframe = self.dataframe
        self.update_analysis()
        
    def update_analysis(self):
        column = self.column_selector.currentText()
        analysis_type = self.analysis_type.currentText()
        df = self.filtered_dataframe if self.dataset_selector.currentText() == "Filtered Dataset" else self.dataframe

        if analysis_type == "":
            self.results_area.clear()
            self.plot_view.hide()
        elif analysis_type == "Value Counts":
            self.show_value_counts(df, column)
        elif analysis_type == "Simplified Value Counts":
            self.show_simplified_value_counts(df, column)
        elif analysis_type == "Descriptive Statistics":
            self.show_descriptive_stats(df, column)
        elif analysis_type == "Distribution Plot":
            self.show_distribution_plot(df, column)

    def toggle_between_inputs(self, index):
        is_between = self.numeric_operator.currentText() == 'between'
        self.numeric_value_2.setVisible(is_between)

    def add_filter(self):
        column = self.filter_column_selector.currentText()
        if self.filter_value_stack.currentIndex() == 1:  # Numeric filter
            operator = self.numeric_operator.currentText()
            value = self.numeric_value.text()
            if operator == 'between':
                value2 = self.numeric_value_2.text()
                if value and value2:
                    filter_text = f"between {value} and {value2}"
                else:
                    QMessageBox.warning(self, "Invalid Input", "Please enter both values for 'between' operator.")
                    return
            else:
                if value:
                    filter_text = f"{operator} {value}"
                else:
                    QMessageBox.warning(self, "Invalid Input", "Please enter a value for the selected operator.")
                    return
        else:  # Categorical filter
            selected_values = [item.text() for item in self.filter_list.selectedItems()]
            if selected_values:
                filter_text = ", ".join(selected_values)
            else:
                QMessageBox.warning(self, "No Selection", "Please select one or more values to filter.")
                return

        if filter_text:
            item = QTreeWidgetItem()
            item.setText(0, column)
            item.setText(1, filter_text)
            self.active_filters_tree.addTopLevelItem(item)
        else:
            QMessageBox.warning(self, "Invalid Filter", "Unable to create filter. Please check your input.")

    def apply_filters(self):
        if self.dataframe is None:
            QMessageBox.warning(self, "No Data", "No dataset is loaded.")
            return

        self.filtered_dataframe = self.dataframe.copy()
        self.filters = {}
        for i in range(self.active_filters_tree.topLevelItemCount()):
            item = self.active_filters_tree.topLevelItem(i)
            column = item.text(0)
            filter_text = item.text(1)
            
            try:
                if pd.api.types.is_numeric_dtype(self.dataframe[column]):
                    if filter_text.startswith('between'):
                        _, value1, _, value2 = filter_text.split()
                        value1, value2 = float(value1), float(value2)
                        self.filtered_dataframe = self.filtered_dataframe[
                            (self.filtered_dataframe[column] >= value1) & 
                            (self.filtered_dataframe[column] <= value2)
                        ]
                    else:
                        operator, value = filter_text.split(maxsplit=1)
                        value = float(value)
                        mask = self.filtered_dataframe[column].apply(
                            lambda x: pd.notna(x) and eval(f"{x} {operator} {value}")
                        )
                        self.filtered_dataframe = self.filtered_dataframe[mask]
                else:
                    values = [v.strip() for v in filter_text.split(",")]
                    self.filtered_dataframe = self.filtered_dataframe[
                        self.filtered_dataframe[column].astype(str).isin(values)
                    ]
                
                self.filters[column] = filter_text
            except Exception as e:
                QMessageBox.warning(self, "Filter Error", f"Error applying filter for column '{column}': {str(e)}")
                continue

        if self.filtered_dataframe.empty:
            QMessageBox.warning(self, "No Results", "The applied filters resulted in an empty dataset.")
            self.filtered_dataframe = self.dataframe.copy()
            self.filters = {}
            self.active_filters_tree.clear()

        self.dataset_selector.setCurrentText("Filtered Dataset")
        self.update_analysis()
        self.update_active_filters_display()
        self.update_row_count_label()

        # Inform the user about the filtering results
        total_rows = len(self.dataframe)
        filtered_rows = len(self.filtered_dataframe)
        QMessageBox.information(self, "Filter Applied", 
                                f"Filtering complete.\n"
                                f"Original rows: {total_rows}\n"
                                f"Filtered rows: {filtered_rows}\n"
                                f"Rows removed: {total_rows - filtered_rows}")
    
    def update_row_count_label(self):
        if self.dataframe is not None:
            total_rows = len(self.dataframe)
            filtered_rows = len(self.filtered_dataframe)
            percentage = (filtered_rows / total_rows) * 100 if total_rows > 0 else 0
            
            label_text = (f"Total rows: {total_rows:,}\n"
                        f"Filtered rows: {filtered_rows:,} ({percentage:.2f}%)")
            
            self.row_count_label.setText(label_text)
        else:
            self.row_count_label.setText("No data loaded")
        
    def filter_values(self, text):
        if self.filter_value_stack.currentIndex() == 0:  # Categorical values
            for i in range(self.filter_list.count()):
                item = self.filter_list.item(i)
                item.setHidden(text.lower() not in item.text().lower())
        else:  # Numeric values
            # For numeric values, we don't filter the input fields
            pass

    def update_active_filters_display(self):
        self.active_filters_tree.clear()
        for column, filter_text in self.filters.items():
            item = QTreeWidgetItem()
            item.setText(0, column)
            item.setText(1, filter_text)
            self.active_filters_tree.addTopLevelItem(item)
            
    def remove_filter(self, item):
        column = item.text(0)
        self.active_filters_tree.invisibleRootItem().removeChild(item)
        if column in self.filters:
            del self.filters[column]
        self.apply_filters()
            
    def update_analysis_options(self):
        analysis_type = self.analysis_type.currentText()
        if analysis_type in ["Value Counts", "Simplified Value Counts"]:
            self.nlargest_widget.show()
        else:
            self.nlargest_widget.hide()
        self.update_analysis()

    def update_nlargest(self, value):
        self.nlargest_value = value
        self.update_analysis()

    def remove_selected_filters(self):
        selected_items = self.active_filters_tree.selectedItems()
        for item in selected_items:
            column = item.text(0)
            self.active_filters_tree.invisibleRootItem().removeChild(item)
            if column in self.filters:
                del self.filters[column]
        self.apply_filters()

    def show_simplified_value_counts(self, df, column):
        value_counts = df[column].value_counts().nlargest(self.nlargest_value)
        total_count = len(df)
        
        output = [f"Top {self.nlargest_value} Value Counts for {column}:"]
        for value, count in value_counts.items():
            percentage = (count / total_count) * 100
            output.append(f"{value}: {count} ({percentage:.2f}%)")

        self.results_area.setText("\n".join(output))
        self.plot_view.hide()

    def show_value_counts(self, df, column):
        value_counts = df[column].value_counts().nlargest(self.nlargest_value)
        total_count = len(df)
        
        # Create a detailed breakdown of value counts
        detailed_counts = {}
        for value, count in value_counts.items():
            filtered_rows = df[df[column] == value]
            details = {}
            for filter_col, filter_values in self.filters.items():
                if filter_col != column:
                    filter_counts = filtered_rows[filter_col].value_counts().nlargest(5)
                    total_filter_count = filter_counts.sum()
                    details[filter_col] = [
                        (fv, fc, fc/total_filter_count*100) 
                        for fv, fc in filter_counts.items()
                    ]
            
            # Cross-reference between filter categories
            cross_ref = {}
            filter_cols = list(self.filters.keys())
            for i in range(len(filter_cols)):
                for j in range(i+1, len(filter_cols)):
                    col1, col2 = filter_cols[i], filter_cols[j]
                    if col1 != column and col2 != column:
                        cross_counts = filtered_rows.groupby([col1, col2]).size().nlargest(5)
                        cross_ref[f"{col1} vs {col2}"] = [
                            (f"{idx[0]} - {idx[1]}", count, count/filtered_rows.shape[0]*100)
                            for idx, count in cross_counts.items()
                        ]
            
            detailed_counts[value] = (count, count/total_count*100, details, cross_ref)

        # Format the output
        output = [f"Top {self.nlargest_value} Value Counts for {column}:"]
        for value, (count, percentage, details, cross_ref) in detailed_counts.items():
            output.append(f"{value}: {count} ({percentage:.2f}%)")
            for filter_col, filter_details in details.items():
                output.append(f"  {filter_col}:")
                for fv, fc, fp in filter_details:
                    output.append(f"    - {fv}: {fc} ({fp:.2f}%)")
            
            output.append("  Cross-references:")
            for cross_key, cross_details in cross_ref.items():
                output.append(f"    {cross_key}:")
                for cv, cc, cp in cross_details:
                    output.append(f"      - {cv}: {cc} ({cp:.2f}%)")
            
            output.append("")  # Add a blank line between entries

        self.results_area.setText("\n".join(output))
        self.plot_view.hide()

    def show_descriptive_stats(self, df, column):
        stats = df[column].describe()
        self.results_area.setText(stats.to_string())
        self.plot_view.hide()

    def show_distribution_plot(self, df, column):
        import plotly.graph_objects as go
        fig = go.Figure()
        fig.add_trace(go.Histogram(x=df[column]))
        fig.update_layout(title=f"Distribution of {column}")
        self.plot_view.setHtml(fig.to_html(include_plotlyjs='cdn'))
        self.plot_view.show()
        self.results_area.clear()

class ClickableBarGraphItem(pg.BarGraphItem):
    """A subclass of BarGraphItem that makes bars clickable."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setAcceptHoverEvents(True)
        self.hovered = False

    def hoverEvent(self, event):
        if event.isEnter():
            x_val = event.pos().x()
            for i, (x, y) in enumerate(zip(self.opts['x'], self.opts['height'])):
                if abs(x_val - x) < 0.5:
                    label = self.opts['x_labels'][i] if 'x_labels' in self.opts else 'Unknown'
                    QToolTip.showText(QCursor.pos(), f"{label}: {y}")
        elif event.isExit():
            QToolTip.hideText()

    def mouseClickEvent(self, event):
        if event.button() == 1 and self.hovered:
            x_val = event.pos().x()
            for x, y in zip(self.opts['x'], self.opts['height']):
                if abs(x_val - x) < 0.5:
                    QMessageBox.information(None, "Bar Value", f"Value: {y}")
        
class MultipleTableSelectionDialog(QDialog):
    def __init__(self, parent, tables):
        super().__init__(parent)
        self.setWindowTitle("Select Tables")
        self.setMinimumSize(300, 400)

        # Main layout
        layout = QVBoxLayout(self)

        # Label
        layout.addWidget(QLabel("Select tables to load:"))

        # Search Bar
        self.search_bar = QLineEdit()
        self.search_bar.setPlaceholderText("Search tables...")
        self.search_bar.textChanged.connect(self.filter_tables)
        layout.addWidget(self.search_bar)

        # Table List
        self.table_list = QListWidget()
        for table in tables:
            item = QListWidgetItem(table)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Unchecked)
            self.table_list.addItem(item)
        layout.addWidget(self.table_list)

        # Select/Deselect All Buttons
        select_deselect_layout = QHBoxLayout()
        btn_select_all = QPushButton("Select All")
        btn_deselect_all = QPushButton("Deselect All")
        btn_select_all.clicked.connect(lambda: self.set_check_states(Qt.CheckState.Checked))
        btn_deselect_all.clicked.connect(lambda: self.set_check_states(Qt.CheckState.Unchecked))
        select_deselect_layout.addWidget(btn_select_all)
        select_deselect_layout.addWidget(btn_deselect_all)
        layout.addLayout(select_deselect_layout)

        # OK and Cancel Buttons
        button_layout = QHBoxLayout()
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        button_layout.addWidget(buttons)
        layout.addLayout(button_layout)

    def set_check_states(self, state):
        for index in range(self.table_list.count()):
            item = self.table_list.item(index)
            if item.flags() & Qt.ItemFlag.ItemIsUserCheckable:
                item.setCheckState(state)

    def get_selected_tables(self):
        return [item.text() for item in self.table_list.findItems("*", Qt.MatchFlag.MatchWildcard) if item.checkState() == Qt.CheckState.Checked]

    def filter_tables(self, text):
        for index in range(self.table_list.count()):
            item = self.table_list.item(index)
            item.setHidden(text.lower() not in item.text().lower())

class DataMergerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.settings = QSettings("MyCompany", "DataMergerApp")
        self.setWindowTitle('Data Merger Tool')
        self.setGeometry(100, 100, 1200, 800)
        
        # Initialize your data structures and widgets
        self.loader_threads = []
        self.conn = None
        self.df1 = None
        self.df2 = None
        self.original_df = None
        self.merged_df = None
        self.aggregated_table = None
        self.map_view = None
        self.active_filters = {}
        self.previous_merges = []
        self.df2_history = []
        self.stats_plot_web_view = QWebEngineView()
        self.general_plot_web_view = QWebEngineView()
        self.df1_post_merge = None
        self.df2_names = []
        self.df1_filename = "Main Data"
        self.df2_filename = "Data for Merging"
        self.stats_table_widget = QTableWidget()
        
        self.setupUI()
        self.create_menu_bar()
        self.repopulate_data_history_combo()

    def setupUI(self):
        self.main_widget = QWidget(self)
        self.setCentralWidget(self.main_widget)
        self.main_layout = QHBoxLayout(self.main_widget)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        self.setup_sidebar()

        self.stacked_widget = QStackedWidget()
        self.main_layout.addWidget(self.stacked_widget)

        self.data_view = self.setup_data_view()
        self.setup_map_view()
        self.setup_statistics_view()
        self.setup_advanced_analytics_view()    

        # Show the Data view by default
        self.show_data_view()
        
    def setup_sidebar(self):
        self.sidebar = ExpandableSidebar()
        self.sidebar.setObjectName("sidebar")
        
        # Apply styles to child widgets (keep the existing styles)
        self.sidebar.setStyleSheet("""
            QLabel {
                color: #c3ccdf;
                font-size: 18px;
                font-weight: bold;
                padding: 20px 10px;
            }
            QPushButton {
                color: #c3ccdf;
                text-align: left;
                padding: 12px 15px;
                border: none;
                border-radius: 0;
                font-size: 14px;
                background-color: transparent;
            }
            QPushButton:hover, QPushButton:checked {
                background-color: #2c313c;
            }
            QPushButton:checked {
                border-left: 3px solid #c3ccdf;
            }
        """)
        
        sidebar_layout = QVBoxLayout(self.sidebar)
        sidebar_layout.setContentsMargins(0, 0, 0, 0)
        sidebar_layout.setSpacing(0)

        # Add app name at the top
        self.app_name_label = QLabel("")
        self.app_name_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.app_name_label.setFixedHeight(60)  # Set a fixed height for the label
        sidebar_layout.addWidget(self.app_name_label)

        # Add a line separator
        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setStyleSheet("background-color: #2d3035;")
        sidebar_layout.addWidget(line)

        icon_base_path = resource_path("icons")

        buttons = [
            ("Data", self.show_data_view, "table.png", None),
            ("Map", self.show_map_view, "globe.png", None),
            ("Statistics", self.show_statistics_view, "bar-chart.png", None),
            ("Analytics", self.show_advanced_analytics_view, "analysis.png", None),
            ("Plots", self.show_plotting_view, "line-chart.png", None),  # New button for plotting
        ]

        self.sidebar_buttons = []
        for button_text, button_function, icon_file, subbuttons in buttons:
            icon_path = os.path.join(icon_base_path, icon_file)
            if os.path.exists(icon_path):
                button = QPushButton(QIcon(icon_path), "")
                button.setCheckable(True)
                button.clicked.connect(button_function)
                button.setIconSize(QSize(24, 24))
                button.setToolTip(button_text)  # Add tooltip for collapsed state
                sidebar_layout.addWidget(button)
                self.sidebar_buttons.append((button, button_text))
            else:
                print(f"Icon not found: {icon_path}")

        sidebar_layout.addStretch()

        self.sidebar.setFixedWidth(60)  # Start collapsed
        self.main_layout.addWidget(self.sidebar)

        # Update button styles and app name visibility when sidebar expands/collapses
        def update_sidebar_state():
            for button, text in self.sidebar_buttons:
                if self.sidebar.expanded:
                    button.setText(text)
                else:
                    button.setText("")
            self.app_name_label.setText("DataMerger" if self.sidebar.expanded else "")

        self.sidebar.animation.finished.connect(update_sidebar_state)
        update_sidebar_state()  # Call once to set initial state
            
    def show_plotting_view(self):
        if not hasattr(self, 'plotting_view'):
            self.plotting_view = QWidget()
            plotting_layout = QVBoxLayout(self.plotting_view)
            
            # Dataset selector
            selector_layout = QHBoxLayout()
            selector_layout.addWidget(QLabel("Select dataset for plotting:"))
            self.plot_dataset_selector = QComboBox()
            self.plot_dataset_selector.addItem("Main Data")
            for name in self.df2_names:
                self.plot_dataset_selector.addItem(name)
            selector_layout.addWidget(self.plot_dataset_selector)
            plotting_layout.addLayout(selector_layout)

            # Initialize PlottingWidget
            self.plotting_widget = PlottingWidget()
            plotting_layout.addWidget(self.plotting_widget)

            # Add the plotting view to the stacked widget
            self.stacked_widget.addWidget(self.plotting_view)

            # Connect signals
            self.plot_dataset_selector.currentIndexChanged.connect(self.update_plotting_dataset)

        # Switch to the plotting view
        self.stacked_widget.setCurrentWidget(self.plotting_view)
        
        # Update sidebar buttons
        self.update_sidebar_buttons("Plots")

        # Set the dataframe for plotting (assuming Main Data is selected by default)
        if self.df1 is not None:
            self.update_plotting_dataset(0)

    def update_plotting_dataset(self, index):
        selected_dataset = self.plot_dataset_selector.currentText()
        if selected_dataset == "Main Data":
            df = self.df1
        else:
            df_index = self.df2_names.index(selected_dataset)
            df = self.df2_history[df_index]

        if df is not None and not df.empty:
            self.plotting_widget.set_dataframe(df)
        else:
            QMessageBox.warning(self, "Error", "No data available for the selected dataset.")
        
    def setup_advanced_analytics_view(self):
        self.advanced_analytics_view = AdvancedAnalyticsView(self)
        self.stacked_widget.addWidget(self.advanced_analytics_view)
        
    def show_advanced_analytics_view(self):
        self.stacked_widget.setCurrentWidget(self.advanced_analytics_view)
        self.update_sidebar_buttons("Advanced Analytics")
        if self.df1 is not None:
            self.advanced_analytics_view.set_dataframe(self.df1)
    
    def setup_data_view(self):
        data_view = QWidget()
        layout = QVBoxLayout(data_view)
        
        # Data Load Section
        data_load_group = QFrame()
        data_load_layout = QVBoxLayout(data_load_group)
        data_load_layout.addWidget(QLabel("Data Load"))
        # Add your data load widgets here
        
        # Merging Options Section
        merging_options_group = QFrame()
        merging_options_layout = QVBoxLayout(merging_options_group)
        merging_options_layout.addWidget(QLabel("Merging Options"))
        # Add your merging options widgets here
        
        # Data View Section
        data_view_splitter = QSplitter(Qt.Orientation.Vertical)
        self.data_view1 = QTableWidget()
        self.data_view2 = QTableWidget()
        data_view_splitter.addWidget(self.data_view1)
        data_view_splitter.addWidget(self.data_view2)
        
        layout.addWidget(data_load_group)
        layout.addWidget(merging_options_group)
        layout.addWidget(data_view_splitter)
        
        self.stacked_widget.addWidget(data_view)
            
    def show_data_view(self):
        # Remove any existing widgets from the data view
        current_widget = self.stacked_widget.widget(0)
        if current_widget:
            self.stacked_widget.removeWidget(current_widget)
            current_widget.deleteLater()

        # Create a new widget for the data view
        data_view = QWidget()
        data_layout = QHBoxLayout(data_view)  # Change to QHBoxLayout

        # Create and add the controls frame on the left
        self.setupControlsFrame()
        data_layout.addWidget(self.controls_frame)

        # Create and add the main tab area on the right
        main_tab_widget = self.setupDataTab()
        data_layout.addWidget(main_tab_widget, 3)  # Give more space to the data view

        # Add the new data view to the stacked widget
        self.stacked_widget.insertWidget(0, data_view)
        self.stacked_widget.setCurrentIndex(0)

        # Update the sidebar buttons
        self.update_sidebar_buttons("Data")

        # Repopulate the data_history_combo
        self.repopulate_data_history_combo()

        # Refresh the data in the tables
        self.refresh_data_views()

    def repopulate_data_history_combo(self):
        self.data_history_combo.clear()
        for name in self.df2_names:
            self.data_history_combo.addItem(name)
        
        # Set the current index to match the current df2
        if self.df2 is not None and self.df2_names:
            current_index = self.df2_history.index(self.df2)
            self.data_history_combo.setCurrentIndex(current_index)
            
    def setup_map_view(self):
        if self.map_view is None:  # Only create the map view if it doesn't exist
            self.map_view = MapView(data_merger_app=self)
            self.map_view.setMinimumSize(800, 600)
            self.stacked_widget.addWidget(self.map_view)

    def show_map_view(self):
        if self.map_view is None:
            self.setup_map_view()  # Create the map view if it doesn't exist
        self.stacked_widget.setCurrentWidget(self.map_view)
        self.update_sidebar_buttons("Map")
        self.map_view.update_map()  # Update the map data
                        
    def setup_statistics_view(self):
        self.statistics_view = QWidget()
        stats_layout = QVBoxLayout(self.statistics_view)

        # Dataset selector and statistics button
        selector_layout = QHBoxLayout()
        selector_layout.addWidget(QLabel("Select dataset for analysis:"))
        self.stats_dataset_selector = QComboBox()
        self.stats_dataset_selector.addItem("Main Data")
        selector_layout.addWidget(self.stats_dataset_selector)
        self.show_stats_button = QPushButton("Show Statistics")
        self.show_stats_button.clicked.connect(self.calculate_and_show_statistics)
        selector_layout.addWidget(self.show_stats_button)
        stats_layout.addLayout(selector_layout)

        # Initialize DataAnalytics
        self.data_analytics = DataAnalytics()
        stats_layout.addWidget(self.data_analytics)

        # Add the statistics view to the stacked widget
        self.stacked_widget.addWidget(self.statistics_view)
                
    def calculate_and_show_statistics(self):
        selected_dataset = self.stats_dataset_selector.currentText()
        if selected_dataset == "Main Data":
            df = self.df1
        else:
            index = self.df2_names.index(selected_dataset)
            df = self.df2_history[index]

        if df is not None and not df.empty:
            self.data_analytics.set_dataframe(df)
            self.data_analytics.update_statistics()  # Now explicitly call update_statistics
        else:
            QMessageBox.warning(self, "Error", "No data available for the selected dataset.")
            
    def show_statistics_view(self):
        # Switch to the statistics view
        self.stacked_widget.setCurrentWidget(self.statistics_view)
        
        # Update sidebar buttons
        self.update_sidebar_buttons("Statistics")
        
    def refresh_data_views(self):
        if self.df1 is not None:
            self.display_data(self.df1, 'data1', self.df1_filename)
        if self.df2 is not None:
            self.display_data(self.df2, 'data2', self.df2_filename)

    def update_sidebar_buttons(self, active_button):
        for button, text in self.sidebar_buttons:
            button.setChecked(text == active_button)

    def create_menu_bar(self):
        menubar = self.menuBar()
        
        # Set the background color of the menu bar (existing code)
        menubar.setStyleSheet("""
            QMenuBar {
                background-color: #2d3035;
                color: white;
            }
            QMenuBar::item:selected {
                background-color: #3a76d8;
            }
            QMenu {
                background-color: #f0f0f0;
                color: black;
            }
            QMenu::item:selected {
                background-color: #4a86e8;
                color: white;
            }
        """)

        # File menu
        file_menu = menubar.addMenu('&File')
        
        # Add actions to File menu
        exit_action = QAction('Exit', self)
        exit_action.setShortcut('Ctrl+Q')
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Data menu
        data_menu = menubar.addMenu('&Data')

        # Add Edit Columns action to Data menu
        edit_columns_action = QAction('Edit Columns', self)
        edit_columns_action.triggered.connect(self.edit_columns)
        data_menu.addAction(edit_columns_action)

        # Export submenu
        export_menu = QMenu('Export Data', self)
        data_menu.addMenu(export_menu)

        # Export main data action
        export_main_action = QAction('Export Main Data', self)
        export_main_action.triggered.connect(self.export_merged_data)
        export_menu.addAction(export_main_action)

        # Export secondary data action
        export_secondary_action = QAction('Export Secondary Data', self)
        export_secondary_action.triggered.connect(self.export_secondary_data)
        export_menu.addAction(export_secondary_action)

        # Help menu
        help_menu = menubar.addMenu('&Help')
        about_action = QAction('About', self)
        about_action.triggered.connect(self.show_about_dialog)
        help_menu.addAction(about_action)

    def show_about_dialog(self):
        QMessageBox.about(self, "About Data Merger", "Data Merger Application\nVersion 1.0\n\nCreated by Your Name")
        
    def setupDataTab(self):
        main_tab_widget = QTabWidget()

        # Create Data View tab
        data_view_tab = QWidget()
        data_view_layout = QVBoxLayout(data_view_tab)

        # Create labels for data views
        self.label_data_view1 = QLabel("Main Data")
        self.label_data_view2 = QLabel("Secondary Data")

        # Create a splitter for the two data views
        data_splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Create and add the two data views with their labels
        view1_widget = QWidget()
        view1_layout = QVBoxLayout(view1_widget)
        view1_layout.addWidget(self.label_data_view1)
        self.data_view1 = QTableView()
        view1_layout.addWidget(self.data_view1)
        
        view2_widget = QWidget()
        view2_layout = QVBoxLayout(view2_widget)
        view2_layout.addWidget(self.label_data_view2)
        self.data_view2 = QTableView()
        view2_layout.addWidget(self.data_view2)

        data_splitter.addWidget(view1_widget)
        data_splitter.addWidget(view2_widget)

        data_view_layout.addWidget(data_splitter)

        # Add the Data View tab to the main tab widget
        main_tab_widget.addTab(data_view_tab, "Data View")

        return main_tab_widget
    ''' 
   def apply_table_settings(self, table_view):
        table_view.setAlternatingRowColors(True)
        table_view.setShowGrid(True)
        
        # Horizontal header settings
        header = table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)
        
        # Ensure headers are visible
        header.setVisible(True)
        
        # Set a minimum section size for the headers
        header.setMinimumSectionSize(100)
        
        # Vertical header settings
        v_header = table_view.verticalHeader()
        v_header.setDefaultSectionSize(24)  # Adjust row height
        
        # Selection settings
        table_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table_view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        
        # Enable sorting
        table_view.setSortingEnabled(True)
    '''
    
    def display_plotly_plot(self, fig, web_engine_view):
        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        web_engine_view.setHtml(plot_html)

    def setupControlsFrame(self):
        self.controls_frame = QFrame()
        self.controls_frame.setFrameShape(QFrame.Shape.NoFrame)
        
        self.controls_layout = QVBoxLayout(self.controls_frame)
        self.controls_layout.setContentsMargins(10, 10, 10, 10)
        self.controls_layout.setSpacing(5)

        self.setupDataLoadGroup()
        self.setupMergeGroup()
        self.setupFilterGroup()

        # Set a fixed width for the controls frame
        self.controls_frame.setFixedWidth(500)  # Adjust this value as needed
        
    def setupDataLoadGroup(self):
        self.data_load_group = QGroupBox("Data Load")
        self.data_load_tab_widget = QTabWidget()  # Tab widget for data load and history
        self.data_load_layout = QVBoxLayout()  # Layout for the 'Load Data' tab

        # 'Load Data' tab
        load_data_widget = QWidget()
        load_data_layout = QVBoxLayout(load_data_widget)
        self.progress_bar = QProgressBar()

        # Create a horizontal layout for buttons
        buttons_layout = QHBoxLayout()

        self.btn_load_data1 = QPushButton('Load main data')
        self.btn_load_data1.clicked.connect(lambda: self.choose_data_source('data1'))
        buttons_layout.addWidget(self.btn_load_data1)

        self.btn_load_data2 = QPushButton('Load secondary data')
        self.btn_load_data2.clicked.connect(lambda: self.choose_data_source('data2'))
        buttons_layout.addWidget(self.btn_load_data2)

        self.btn_clear_all = QPushButton('Clear All Data')
        self.btn_clear_all.clicked.connect(self.clear_all_data)
        buttons_layout.addWidget(self.btn_clear_all)

        # Add the progress bar and buttons layout to the load data layout
        load_data_layout.addWidget(self.progress_bar)
        load_data_layout.addLayout(buttons_layout)

        # 'Data History' tab
        data_history_widget = QWidget()
        data_history_layout = QVBoxLayout(data_history_widget)
        self.data_history_combo = QComboBox()
        self.data_history_combo.currentIndexChanged.connect(self.switch_to_selected_data)
        data_history_layout.addWidget(QLabel("Select Data for Merging:"))
        data_history_layout.addWidget(self.data_history_combo)

        # Add a button to remove selected secondary data
        self.remove_secondary_data_btn = QPushButton("Remove Selected Secondary Data")
        self.remove_secondary_data_btn.clicked.connect(self.remove_selected_secondary_data)
        data_history_layout.addWidget(self.remove_secondary_data_btn)

        # Add tabs to the tab widget
        self.data_load_tab_widget.addTab(load_data_widget, "Load Data")
        self.data_load_tab_widget.addTab(data_history_widget, "Secondary Data")

        # Add the tab widget to the group box layout
        self.data_load_group.setLayout(self.data_load_layout)
        self.data_load_layout.addWidget(self.data_load_tab_widget)
        self.controls_layout.addWidget(self.data_load_group)

    def remove_selected_secondary_data(self):
        current_index = self.data_history_combo.currentIndex()
        if current_index >= 0:
            data_name = self.data_history_combo.currentText()
            confirm = QMessageBox.question(self, "Confirm Removal", 
                                        f"Are you sure you want to remove '{data_name}' from memory?",
                                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if confirm == QMessageBox.StandardButton.Yes:
                # Remove the data from df2_history and df2_names
                del self.df2_history[current_index]
                del self.df2_names[current_index]
                # Remove the item from the combo boxes
                self.data_history_combo.removeItem(current_index)
                index = self.stats_dataset_selector.findText(data_name)
                if index >= 0:
                    self.stats_dataset_selector.removeItem(index)
                
                # Update df2 and display
                if self.df2_history:
                    # If there's still data, switch to the first available dataset
                    self.df2 = self.df2_history[0]
                    new_data_name = self.df2_names[0]
                    self.display_data(self.df2, 'data2', new_data_name)
                    self.update_secondary_data_label(new_data_name)
                    self.data_history_combo.setCurrentIndex(0)
                else:
                    # If all data is removed
                    self.df2 = None
                    self.clear_table_view(self.data_view2)
                    self.update_secondary_data_label("No data loaded")
                
                # Optionally, trigger garbage collection
                import gc
                gc.collect()
                QMessageBox.information(self, "Data Removed", f"'{data_name}' has been removed from memory.")
                
    def clear_table_view(self, table_view):
        model = QStandardItemModel()
        table_view.setModel(model)
        
    def setupMergeGroup(self):
        merge_group = QGroupBox("Merging Options")
        merge_layout = QVBoxLayout(merge_group)
        self.setupMergeControls(merge_layout)
        self.controls_layout.addWidget(merge_group)

    def setupMergeControls(self, layout):
        self.btn_merge = QPushButton('Merge Data')
        self.btn_merge.clicked.connect(self.show_merge_dialog)
        self.btn_revert_merge = QPushButton('Revert Merge')
        self.btn_revert_merge.clicked.connect(self.revert_merge)
        self.btn_revert_merge.setEnabled(False)
        self.column_select1 = QComboBox()
        self.column_select2 = QComboBox()
        self.columns_to_add_selector = QListWidget()
        self.columns_to_add_selector.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        layout.addWidget(QLabel("Select merge column from main data:"))
        layout.addWidget(self.column_select1)
        layout.addWidget(QLabel("Select merge column from secondary data:"))
        layout.addWidget(self.column_select2)
        layout.addWidget(QLabel("Select additional columns from secondary data to add to main data:"))
        layout.addWidget(self.columns_to_add_selector)
        merge_buttons_layout = QHBoxLayout()
        merge_buttons_layout.addWidget(self.btn_merge)
        merge_buttons_layout.addWidget(self.btn_revert_merge)
        layout.addLayout(merge_buttons_layout)
        
    def show_merge_dialog(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("Select Merge Type")
        layout = QVBoxLayout(dialog)

        # Create radio buttons for merge types
        merge_types = ['left', 'inner', 'outer', 'right']
        button_group = QButtonGroup(dialog)
        for i, merge_type in enumerate(merge_types):
            radio_button = QRadioButton(merge_type)
            button_group.addButton(radio_button, i)
            layout.addWidget(radio_button)
            if i == 0:  # Set the first option as default
                radio_button.setChecked(True)

        # Add tooltip to the dialog
        tooltip_text = ("Select the type of merge operation:\n"
                        "Left - Use keys from left frame only\n"
                        "Inner - Use intersection of keys\n"
                        "Outer - Use union of keys\n"
                        "Right - Use keys from right frame only")
        dialog.setToolTip(tooltip_text)

        # Create Apply button
        apply_button = QPushButton("Apply")
        apply_button.clicked.connect(dialog.accept)
        layout.addWidget(apply_button)

        if dialog.exec() == QDialog.DialogCode.Accepted:
            selected_merge_type = merge_types[button_group.checkedId()]
            self.merge_data(selected_merge_type)
            
    def setupFilterGroup(self):
        filter_group = QGroupBox("Column Management, Data Filtration and Aggregation")
        filter_layout = QVBoxLayout(filter_group)

        # Create a horizontal layout for the main action buttons
        action_buttons_layout = QHBoxLayout()

        self.btn_open_filter = QPushButton('Open Filter Menu')
        self.btn_open_filter.clicked.connect(self.open_filter_dialog)
        action_buttons_layout.addWidget(self.btn_open_filter)

        self.btn_group_aggregate = QPushButton('Group and Aggregate Data')
        self.btn_group_aggregate.clicked.connect(self.group_and_aggregate)
        action_buttons_layout.addWidget(self.btn_group_aggregate)

        filter_layout.addLayout(action_buttons_layout)

        self.btn_drop_columns = QPushButton('Drop Selected Columns')
        filter_layout.addWidget(QLabel("Select columns to drop from merged data:"))
        self.columns_to_drop_selector = QListWidget()
        self.columns_to_drop_selector.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        filter_layout.addWidget(self.columns_to_drop_selector)
        filter_layout.addWidget(self.btn_drop_columns)
        self.btn_drop_columns.clicked.connect(self.drop_columns)

        self.controls_layout.addWidget(filter_group)  # Add filter_group to the main controls_layout

    def choose_data_source(self, key):
        if key == 'data1' and self.df1 is not None:
            response = QMessageBox.question(
                self, "Confirm Load New Main Data",
                "Loading new main data will replace the current data and remove all merges/filters. Do you want to continue?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            if response == QMessageBox.StandardButton.No:
                QMessageBox.information(self, "Cancelled", "Loading new main data cancelled.")
                return

        last_server = self.settings.value("last_server", "")
        last_database = self.settings.value("last_database", "")
        dialog = DataSourceDialog(self, last_server, last_database)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            # Clear filters and reset df1_post_merge before loading new data
            self.clear_filters_and_reset_post_merge()
            if dialog.localFileButton.isChecked():
                self.load_data_from_file(key)
            elif dialog.sqlDatabaseButton.isChecked():
                self.connect_and_load_from_sql(dialog.serverInput.text(), dialog.databaseInput.text(), key)

    def clear_filters_and_reset_post_merge(self):
        self.active_filters = {}  # Clear active filters
        self.df1_post_merge = None  # Reset df1_post_merge

    def connect_and_load_from_sql(self, server, database, key):
        drivers = ["ODBC Driver 17 for SQL Server", "ODBC Driver 13 for SQL Server", "SQL Server"]
        for driver in drivers:
            try:
                self.conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
                self.conn = pyodbc.connect(self.conn_str)
                print(f"Connected using {driver}")
                self.load_data_from_multiple_tables(key)
                self.settings.setValue("last_server", server)
                self.settings.setValue("last_database", database)
                return
            except Exception as e:
                print(f"Failed to connect using {driver}: {e}")

        QMessageBox.critical(self, "Connection Error", "Failed to connect to database using any known drivers.")

    def load_data_from_multiple_tables(self, key):
        cursor = self.conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME")
        tables = sorted([table[0] for table in cursor.fetchall()])

        selection_dialog = MultipleTableSelectionDialog(self, tables)
        if selection_dialog.exec() == QDialog.DialogCode.Accepted:
            selected_tables = selection_dialog.get_selected_tables()
            for table_name in selected_tables:
                columns = self.fetch_columns_for_table(table_name)
                if columns:
                    self.load_data_for_table(table_name, columns, key)

            if key == 'data2' and len(self.df2_history) > 0:
                self.switch_to_selected_data()

    def fetch_columns_for_table(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        columns = ['[' + column[0] + ']' for column in cursor.fetchall()]
        cursor.execute("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = ? AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
        """, [table_name])
        primary_keys = ['[' + pk[0] + ']' for pk in cursor.fetchall()]

        column_selector = QDialog(self)
        column_selector.setWindowTitle("Select Columns to Keep")
        layout = QVBoxLayout(column_selector)

        layout.addWidget(QLabel(f"Select columns to keep from {table_name}:"))

        column_list = QListWidget()
        for column in columns:
            item = QListWidgetItem(column)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Checked if column not in primary_keys else Qt.CheckState.Checked)
            if column in primary_keys:
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEnabled)
            column_list.addItem(item)
        layout.addWidget(column_list)

        select_deselect_buttons = QHBoxLayout()
        btn_select_all = QPushButton("Select All")
        btn_deselect_all = QPushButton("Deselect All")
        btn_select_all.clicked.connect(lambda: self.set_check_states_exclude_primary(column_list, Qt.CheckState.Checked, primary_keys))
        btn_deselect_all.clicked.connect(lambda: self.set_check_states_exclude_primary(column_list, Qt.CheckState.Unchecked, primary_keys))
        select_deselect_buttons.addWidget(btn_select_all)
        select_deselect_buttons.addWidget(btn_deselect_all)
        layout.addLayout(select_deselect_buttons)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(column_selector.accept)
        buttons.rejected.connect(column_selector.reject)
        layout.addWidget(buttons)

        if column_selector.exec() == QDialog.DialogCode.Accepted:
            selected_columns = [item.text() for item in column_list.findItems("*", Qt.MatchFlag.MatchWildcard) if item.checkState() == Qt.CheckState.Checked]
            return selected_columns
        return []

    def load_data_for_table(self, table_name, columns, key):
        selected_columns_str = ', '.join(columns)
        conn_str = self.conn_str  # Assuming `self.conn_str` is set in `connect_and_load_from_sql`

        apply_date_filter, _ = QInputDialog.getItem(
            self, "Apply Date Filter?", "Do you want to apply a date filter?",
            ["No", "Yes"], 0, False
        )

        if apply_date_filter == "Yes":
            self.apply_date_filter(table_name, key, selected_columns_str, columns)
        else:
            loader = SQLDataLoader(
                conn_str, table_name, selected_columns=selected_columns_str
            )
            self.setup_loader(loader, key, table_name)


    def apply_date_filter(self, table_name, key, selected_columns_str, selected_columns):
        date_columns_query = """
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ? AND DATA_TYPE IN ('date', 'datetime', 'datetime2', 
        'datetimeoffset', 'smalldatetime', 'time', 'timestamp')
        """
        conn = pyodbc.connect(self.conn_str)
        cursor = conn.cursor()
        cursor.execute(date_columns_query, [table_name])
        all_date_columns = [f'[{row[0]}]' for row in cursor.fetchall()]
        date_columns = list(set(all_date_columns) & set(selected_columns))

        if not date_columns:
            QMessageBox.information(self, "No Date Columns", "No date columns were found in the selected table.")
            return

        date_column, ok = QInputDialog.getItem(
            self, "Select Date Column", "Choose a date column for filtering:",
            date_columns, 0, False
        )
        if not ok:
            return

        start_date, ok = QInputDialog.getText(
            self, "Start Date", "Enter start date (YYYY-MM-DD):",
            QLineEdit.EchoMode.Normal, "e.g., 2020-01-01"
        )
        if not ok:
            return

        end_date, ok = QInputDialog.getText(
            self, "End Date (optional)", "Enter end date (YYYY-MM-DD), leave blank if open-ended:",
            QLineEdit.EchoMode.Normal, "e.g., 2025-12-31"
        )
        if not ok:
            end_date = ''

        count_query = f"SELECT COUNT(*) FROM {table_name} WHERE {date_column} >= ?"
        params = [start_date]
        if end_date.strip():
            count_query += f" AND {date_column} <= ?"
            params.append(end_date.strip())

        cursor.execute(count_query, params)
        filtered_row_count = cursor.fetchone()[0]

        if filtered_row_count == 0:
            QMessageBox.information(self, "No Data", "No data available for the selected date range.")
            return

        QMessageBox.information(self, "Filtered Data Info", f"Rows after applying date filter: {filtered_row_count}")
        loader = SQLDataLoader(self.conn_str, table_name, date_column, start_date, (end_date.strip() if end_date.strip() else None), selected_columns=selected_columns_str)
        self.setup_loader(loader, key, table_name)


    def set_check_states_exclude_primary(self, list_widget, state, primary_keys):
        for index in range(list_widget.count()):
            item = list_widget.item(index)
            if item.flags() & Qt.ItemFlag.ItemIsUserCheckable and item.text() not in primary_keys:
                    item.setCheckState(state)

    def load_data_from_file(self, key):
        if key == 'data1':
            file_path, _ = QFileDialog.getOpenFileName(
                self, "Open File", "", 
                "CSV Files (*.csv);;Excel Files (*.xlsx);;Shapefiles (*.shp)"
            )
            if file_path:
                file_name = os.path.basename(file_path)
                self.df1_filename = file_name
                loader = DataLoader(file_path)
                self.setup_loader(loader, key, file_name)
        elif key == 'data2':
            file_dialog = QFileDialog(self)
            file_dialog.setFileMode(QFileDialog.FileMode.ExistingFiles)
            file_dialog.setNameFilter("CSV Files (*.csv);;Excel Files (*.xlsx)")
            if file_dialog.exec() == QDialog.DialogCode.Accepted:
                file_paths = file_dialog.selectedFiles()
                for file_path in file_paths:
                    file_name = os.path.basename(file_path)
                    loader = DataLoader(file_path)
                    self.setup_loader(loader, key, file_name)

    def setup_loader(self, loader, key, data_name=None):
        self.loader_threads.append(loader)
        loader.progress.connect(self.progress_bar.setValue)
        loader.dataLoaded.connect(lambda df: self.handle_data_loaded(df, key, data_name))
        loader.finished.connect(loader.deleteLater)
        loader.start()

    def handle_data_loaded(self, df, key, data_name):
        if key == 'data1':
            self.df1 = df
            if self.original_df is None or self.original_df.empty:
                self.original_df = df.copy()
            self.display_data(self.df1, 'data1', data_name if data_name else self.df1_filename)
            self.update_map_data()
            self.data_analytics.set_dataframe(df)
            self.advanced_analytics_view.set_dataframe(df)  # This will update the dataset selector
        if key == 'data2':
            self.df2 = df
            self.df2_history.append(df)
            if data_name is None:
                data_name = f"Dataset {len(self.df2_history)}"
            self.df2_names.append(data_name)
            self.data_history_combo.addItem(data_name)
            if hasattr(self, 'stats_dataset_selector'):
                self.stats_dataset_selector.addItem(data_name)
            self.display_data(self.df2, 'data2', data_name)
            self.update_secondary_data_label(data_name)
        
        # Update the Advanced Analytics view if it exists
        if hasattr(self, 'advanced_analytics_view'):
            self.advanced_analytics_view.update_dataset_selector()

    def switch_to_selected_data(self):
        index = self.data_history_combo.currentIndex()
        if 0 <= index < len(self.df2_history):
            self.df2 = self.df2_history[index]
            data_name = self.df2_names[index]
            self.display_data(self.df2, 'data2', data_name)
            self.update_secondary_data_label(data_name)
            
            # Update the Advanced Analytics view if it exists
            if hasattr(self, 'advanced_analytics_view'):
                self.advanced_analytics_view.update_dataset_selector()

    def update_secondary_data_label(self, data_name):
        self.label_data_view2.setText(f"{data_name}")
        # Update the tab text if you're using tabs
        if hasattr(self, 'tab_widget2'):
            self.tab_widget2.setTabText(0, f"{data_name}")
        
    def remove_loader(self, loader):
        if loader in self.loader_threads:
            self.loader_threads.remove(loader)
        else:
            print("Loader already removed or not found:", loader)
                                                
    def update_table_info(self, label, dataframe, data_name, filename, matches_count=None):
        if dataframe is not None and not dataframe.empty:
            row_count = dataframe.shape[0]
            col_count = dataframe.shape[1]
            text = f"{data_name} - Rows: {row_count}, Columns: {col_count}"
            if matches_count:
                text += f", Matches: {matches_count}"
            label.setText(text)
        else:
            label.setText(f"{data_name} - No data loaded")
                
    def setup_virtual_scrolling(self, table_view, dataframe):
        model = DataFrameModel(dataframe)
        table_view.setModel(model)
        
        # Set up a custom vertical header
        vertical_header = table_view.verticalHeader()
        vertical_header.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        vertical_header.setDefaultSectionSize(30)  # Adjust as needed
        
        # Set up horizontal header
        horizontal_header = table_view.horizontalHeader()
        horizontal_header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        
        # Enable sorting
        table_view.setSortingEnabled(True)
        table_view.sortByColumn(-1, Qt.SortOrder.AscendingOrder)  # Initial sort state
        
        # Connect sorting signal
        table_view.horizontalHeader().sectionClicked.connect(self.handle_sort)

    def handle_sort(self, logical_index):
        table_view = self.sender().parent()
        model = table_view.model()
        
        if model._sort_column == logical_index:
            order = Qt.SortOrder.DescendingOrder if model._sort_order == Qt.SortOrder.AscendingOrder else Qt.SortOrder.AscendingOrder
        else:
            order = Qt.SortOrder.AscendingOrder
        
        model.sort(logical_index, order)
        table_view.reset()

    def display_data(self, df, key, filename):
        model = DataFrameModel(df)
        if key == 'data1':
            self.data_view1.setModel(model)
            self.data_view1.setSortingEnabled(True)
            self.df1 = df
            if not hasattr(self, 'df1_post_merge') or self.df1_post_merge is None:
                self.df1_post_merge = df.copy()
            # Only update df1_filename if it's not set or is the default value
            if not hasattr(self, 'df1_filename') or self.df1_filename == "Main Data":
                self.df1_filename = filename
            self.update_table_info(self.label_data_view1, df, self.df1_filename, self.df1_filename)
            self.update_column_selectors()
            self.data_analytics.set_dataframe(df)
            self.update_map_data()
            self.setup_virtual_scrolling(self.data_view1, df)

            if hasattr(self, 'tab_widget1'):
                self.tab_widget1.setTabText(0, self.df1_filename)  # Use the stored filename
        elif key == 'data2':
            self.data_view2.setModel(model)
            self.data_view2.setSortingEnabled(True)
            if hasattr(self, 'column_select2'):
                self.column_select2.clear()
                self.column_select2.addItems(df.columns)
            self.df2 = df
            # Only update df2_filename if it's not set or is the default value
            if not hasattr(self, 'df2_filename') or self.df2_filename == "Data for Merging":
                self.df2_filename = filename
            self.update_table_info(self.label_data_view2, df, self.df2_filename, self.df2_filename)
            self.update_column_selectors()
            self.setup_virtual_scrolling(self.data_view2, df)

            if hasattr(self, 'tab_widget2'):
                self.tab_widget2.setTabText(0, self.df2_filename)  # Use the stored filename
                
    def update_column_selectors(self):
        # Clear selectors if they exist
        for selector in ['column_select1', 'column_select2', 'columns_to_add_selector', 'columns_to_drop_selector']:
            if hasattr(self, selector):
                getattr(self, selector).clear()

        if self.df1 is not None:
            if hasattr(self, 'column_select1'):
                self.column_select1.addItems(list(self.df1.columns))
            if hasattr(self, 'columns_to_drop_selector'):
                self.columns_to_drop_selector.addItems(list(self.df1.columns))

        if self.df2 is not None:
            if hasattr(self, 'column_select2'):
                self.column_select2.addItems(list(self.df2.columns))
            
            if self.df1 is not None and hasattr(self, 'columns_to_add_selector'):
                # Normalize all column names to lowercase for comparison
                df1_columns_lower = {col.lower(): col for col in self.df1.columns}
                df2_columns_lower = {col.lower(): col for col in self.df2.columns}

                # Find common and additional columns based on normalized names
                common_columns_lower = set(df1_columns_lower.keys()).intersection(df2_columns_lower.keys())
                additional_columns_lower = set(df2_columns_lower.keys()).difference(df1_columns_lower.keys())

                # Map back to original column names in df2 for display
                common_columns = [df2_columns_lower[col] for col in common_columns_lower]
                additional_columns = [df2_columns_lower[col] for col in additional_columns_lower]

                # Create and style header for common columns
                self.add_header_to_selector("Common Columns", self.columns_to_add_selector)
                for column in sorted(common_columns):
                    self.columns_to_add_selector.addItem(QListWidgetItem(column))

                # Create and style header for additional columns
                self.add_header_to_selector("Additional Columns", self.columns_to_add_selector)
                for column in sorted(additional_columns):
                    item = QListWidgetItem(column)
                    item.setForeground(QColor('green'))  # Highlight additional columns
                    self.columns_to_add_selector.addItem(item)
            elif hasattr(self, 'columns_to_add_selector'):
                # If df1 is not loaded, list all columns from df2
                self.columns_to_add_selector.addItems(sorted(self.df2.columns))

    def add_header_to_selector(self, header_text, selector):
        header = QListWidgetItem(header_text)
        header.setFlags(Qt.ItemFlag.NoItemFlags)  # Make header non-selectable
        font = QFont()
        font.setBold(True)
        font.setPointSize(12)  # Increase font size
        header.setFont(font)
        selector.addItem(header)
            

    def export_merged_data(self):
        if self.df1 is not None:
            dialog = QFileDialog(self, "Save File")
            dialog.setNameFilter("CSV Files (*.csv);;Excel Files (*.xlsx)")
            dialog.setAcceptMode(QFileDialog.AcceptMode.AcceptSave)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                filePath = dialog.selectedFiles()[0]
                if filePath:
                    if filePath.endswith('.csv'):
                        self.df1.to_csv(filePath, index=False)
                    elif filePath.endswith('.xlsx'):
                        self.df1.to_excel(filePath, index=False)
                    QMessageBox.information(self, "Export Successful", f"Data successfully exported to {filePath}.")
        else:
            QMessageBox.warning(self, "Export Error", "No main data to export.")
            
    def export_secondary_data(self):
        if not self.df2_names:
            QMessageBox.warning(self, "Export Error", "No secondary data available to export.")
            return

        export_dialog = QDialog(self)
        export_dialog.setWindowTitle("Export Secondary Data")
        layout = QVBoxLayout(export_dialog)

        # Create a list widget to display secondary datasets
        list_widget = QListWidget()
        for name in self.df2_names:
            item = QListWidgetItem(name)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Unchecked)
            list_widget.addItem(item)

        layout.addWidget(QLabel("Select secondary datasets to export:"))
        layout.addWidget(list_widget)

        # Add export button
        export_button = QPushButton("Export")
        export_button.clicked.connect(export_dialog.accept)
        layout.addWidget(export_button)

        if export_dialog.exec() == QDialog.DialogCode.Accepted:
            selected_datasets = [list_widget.item(i).text() for i in range(list_widget.count()) 
                                if list_widget.item(i).checkState() == Qt.CheckState.Checked]
            
            if not selected_datasets:
                QMessageBox.warning(self, "Export Error", "No datasets selected for export.")
                return

            # Ask user to select a directory for exporting multiple files
            export_dir = QFileDialog.getExistingDirectory(self, "Select Export Directory")
            if not export_dir:
                return  # User cancelled directory selection

            for dataset_name in selected_datasets:
                dataset = self.df2_history[self.df2_names.index(dataset_name)]
                self.export_single_dataset(dataset, dataset_name, export_dir)

        QMessageBox.information(self, "Export Complete", "Selected secondary datasets have been exported.")

    def export_single_dataset(self, df, filename, export_dir):
        file_path = os.path.join(export_dir, f"{filename}.csv")
        df.to_csv(file_path, index=False)
        print(f"Exported {filename} to {file_path}")  # For debugging

                
    def drop_columns(self):
        selected_items = self.columns_to_drop_selector.selectedItems()
        columns_to_drop = [item.text() for item in selected_items]

        if self.df1 is not None and not self.df1.empty:
            # Drop the selected columns through the model
            model = self.data_view1.model()
            if model:
                model.dropColumns(columns_to_drop)
                self.update_column_selectors()  # Reflect changes in UI selectors
                self.update_map_data()  # Update the map after applying filters
            QMessageBox.information(self, "Columns Dropped", "Selected columns have been dropped from the main data.")
        else:
            QMessageBox.warning(self, "Drop Columns Error", "No main data available to drop columns from.")
            
    def edit_columns(self):
        if self.df1 is not None:
            dialog = ColumnEditorDialog(self.df1.columns.tolist(), self)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                new_columns = dialog.get_updated_columns()
                self.update_dataframe_columns(new_columns)
        else:
            QMessageBox.warning(self, "No Data", "Please load data before editing columns.")

    def update_dataframe_columns(self, new_columns):
        # Update the DataFrame
        self.df1 = self.df1.reindex(columns=new_columns)
        
        # Update the model
        model = self.data_view1.model()
        if isinstance(model, DataFrameModel):
            model.reorder_columns(new_columns)
        else:
            # If the model is not a DataFrameModel, create a new one
            model = DataFrameModel(self.df1)
            self.data_view1.setModel(model)
        
        # Update other UI elements that depend on column names
        self.update_column_related_ui()

    def update_column_related_ui(self):
        # Update column selectors and other UI elements
        self.column_select1.clear()
        self.column_select1.addItems(self.df1.columns)
        
        self.columns_to_drop_selector.clear()
        self.columns_to_drop_selector.addItems(self.df1.columns)
        
        # Update any other UI elements that display column names
        # For example, if you have filters or other selectors:
        # self.update_filter_selectors()
        # self.update_merge_selectors()
        
        # Refresh the data view
        self.data_view1.reset()
        self.data_view1.resizeColumnsToContents()

    def merge_data(self, merge_type):
        if self.df1 is None or self.df2 is None:
            QMessageBox.warning(self, "Data Error", "Please load both datasets before attempting to merge.")
            return

        merge_column1 = self.column_select1.currentText()  # User-selected primary key for df1
        merge_column2 = self.column_select2.currentText()  # User-selected primary key for df2
        # merge_type is now a parameter, so we don't need to get it from self.merge_type_select

        # Check for duplicate entries in the secondary dataframe
        duplicates = self.df2.duplicated(subset=[merge_column2]).sum()
        if duplicates > 0:
            # Warn the user about potential additional rows
            warning_msg = (f"There are {duplicates} duplicate entries in the secondary dataset. "
                        "Merging may result in additional rows in the main dataset. Do you want to continue?")
            reply = QMessageBox.question(self, 'Warning', warning_msg, 
                                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, 
                                        QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.No:
                return

            # Prompt the user to specify how to handle duplicates
            additional_column, ok = QInputDialog.getItem(
                self, "Duplicate Entries Detected",
                "Multiple entries for the same key detected in the merge data. Please select a secondary column to differentiate:",
                self.df2.columns.tolist(), 0, False
            )
            if not ok:
                return  # User canceled the operation
            
        selected_items = self.columns_to_add_selector.selectedItems()
        additional_columns = [item.text() for item in selected_items]

        # Build the list of columns to include from df2
        df2_columns_to_merge = [merge_column2]
        if 'additional_column' in locals():
            if additional_column in self.df2.columns:
                df2_columns_to_merge.append(additional_column)
            if additional_column not in additional_columns:
                additional_columns.append(additional_column)
        df2_columns_to_merge.extend(additional_columns)
        df2_columns_to_merge = list(dict.fromkeys(df2_columns_to_merge))  # Remove any duplicates

        # Perform the merge
        if 'additional_column' in locals() and additional_column in self.df1.columns and additional_column in self.df2.columns:
            new_df1 = pd.merge(
                self.df1, self.df2[df2_columns_to_merge],
                left_on=[merge_column1, additional_column], right_on=[merge_column2, additional_column],
                how=merge_type, indicator=True, suffixes=('', '_dup')
            )
        else:
            new_df1 = pd.merge(
                self.df1, self.df2[df2_columns_to_merge],
                left_on=merge_column1, right_on=merge_column2,
                how=merge_type, indicator=True, suffixes=('', '_dup')
            )

        # ... (rest of the function remains the same)

        # Clean up any duplicate columns if not needed
        new_df1.drop(columns=[col for col in new_df1.columns if col.endswith('_dup')], inplace=True)

        # Calculate statistics
        rows_before = len(self.df1)
        rows_after = len(new_df1)
        rows_added = max(0, rows_after - rows_before)
        rows_removed = max(0, rows_before - rows_after)

        # Identify new columns added during the merge
        new_columns = [col for col in new_df1.columns if col not in self.df1.columns and col != '_merge']
        
        # Count rows without new data for each new column
        empty_counts = {}
        no_match_counts = {}
        empty_with_match_counts = {}
        for col in new_columns:
            empty_counts[col] = new_df1[col].isna().sum()
            no_match_counts[col] = new_df1[(new_df1['_merge'] == 'left_only') & (new_df1[col].isna())].shape[0]
            empty_with_match_counts[col] = empty_counts[col] - no_match_counts[col]

        # Count rows without any new data across all new columns
        rows_without_new_data = new_df1[new_columns].isna().all(axis=1).sum() if new_columns else 0
        rows_no_match = new_df1[new_df1['_merge'] == 'left_only'].shape[0]
        rows_empty_with_match = rows_without_new_data - rows_no_match

        self.previous_merges.append(self.df1.copy())  # Save current state for undo functionality
        self.df1 = new_df1.drop(columns=['_merge'])  # Update main data with merged result and remove indicator column
        self.df1_post_merge = self.df1.copy()  # Save the merged data for future operations
        self.display_data(self.df1, 'data1', "Main Data Updated")  # Refresh the main data view
        self.btn_revert_merge.setEnabled(True)  # Enable the revert button after successful merge
        
        merge_info = (f"Data has been successfully merged and updated.\n"
                    f"Merge type: {merge_type}\n"
                    f"Original rows: {rows_before}\n"
                    f"Rows after merge: {rows_after}\n"
                    f"Rows added: {rows_added}\n"
                    f"Rows removed: {rows_removed}\n"
                    f"Rows without any new data: {rows_without_new_data} ({rows_without_new_data/rows_after:.2%})\n"
                    f"  - No match found: {rows_no_match} ({rows_no_match/rows_after:.2%})\n"
                    f"  - Match found, but no data: {rows_empty_with_match} ({rows_empty_with_match/rows_after:.2%})\n"
                    f"New columns added: {', '.join(new_columns)}\n\n"
                    f"Empty counts for new columns:")
        
        for col in new_columns:
            total_empty = empty_counts[col]
            no_match = no_match_counts[col]
            empty_with_match = empty_with_match_counts[col]
            
            merge_info += f"\n{col}:"
            merge_info += f"\n  Total empty: {total_empty} ({total_empty/rows_after:.2%})"
            merge_info += f"\n  No match: {no_match} ({no_match/rows_after:.2%})"
            merge_info += f"\n  Match, but no data: {empty_with_match} ({empty_with_match/rows_after:.2%})"

        QMessageBox.information(self, "Merge Successful", merge_info)
        
        self.update_column_selectors()
        self.update_map_data()  # Update the map after applying filters

    def revert_merge(self):
        if self.previous_merges:
            self.df1 = self.previous_merges.pop()  # Restore the previous state of main data
            self.display_data(self.df1, 'data1', "Main Data Reverted")  # Update the main data view
            self.update_column_selectors()
            QMessageBox.information(self, "Revert Successful", "The last merge has been reverted.")
            if not self.previous_merges:
                self.btn_revert_merge.setEnabled(False)  # Disable the button if no more merges to revert
        else:
            QMessageBox.information(self, "Revert Not Possible", "No previous merges to revert to.")

    def open_filter_dialog(self):
        if self.df1 is not None:
            dataframes = {'Main Data': self.df1}
            dataframes.update({name: df for name, df in zip(self.df2_names, self.df2_history)})
            dialog = FilterDialog(dataframes, self, self.active_filters)
            dialog.filtersCleared.connect(self.handle_filters_cleared)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                new_filters = dialog.get_filters()
                self.apply_filters(new_filters)

    def handle_filters_cleared(self, dataset_name):
        if dataset_name == 'Main Data':
            if self.df1_post_merge is not None:
                self.display_data(self.df1_post_merge, 'data1', "Data view after clearing filters")
            else:
                self.display_data(self.df1, 'data1', "Main Data")
        else:
            # Handle clearing filters for secondary datasets
            index = self.df2_names.index(dataset_name)
            self.display_data(self.df2_history[index], 'data2', dataset_name)
        
        # Update active filters
        if dataset_name in self.active_filters:
            del self.active_filters[dataset_name]

        self.update_map_data()  # Update map if necessary

    def apply_filters(self, new_filters):
        filtered_dfs = {}

        if "Main Data" in new_filters:
            # Use df1_post_merge if it exists and is not None, otherwise use df1
            df_to_filter = self.df1_post_merge if self.df1_post_merge is not None else self.df1
            if df_to_filter is not None:
                filtered_dfs["Main Data"] = self.apply_single_filter(df_to_filter, new_filters["Main Data"])
                self.display_data(filtered_dfs["Main Data"], 'data1', self.df1_filename)
        
        for dataset_name in self.df2_names:
            if dataset_name in new_filters and self.df2_history is not None:
                index = self.df2_names.index(dataset_name)
                filtered_df = self.apply_single_filter(self.df2_history[index], new_filters[dataset_name])
                filtered_dfs[dataset_name] = filtered_df
                if dataset_name == self.data_history_combo.currentText():
                    self.display_data(filtered_df, 'data2', dataset_name)

        self.active_filters = new_filters
        self.update_map_data()

    def apply_single_filter(self, dataframe, filters):
        filtered_df = dataframe.copy()
        for column, conditions in filters.items():
            if column == 'cross_reference':
                other_dataset = conditions['dataset']
                other_column = conditions['column']
                if other_dataset == 'Main Data':
                    other_df = self.df1
                elif other_dataset in self.df2_names:
                    other_df = self.df2_history[self.df2_names.index(other_dataset)]
                else:
                    continue  # Skip if dataset is not recognized

                # Ensure the other DataFrame and column exist
                if other_df is not None and other_column in other_df.columns:
                    other_column_values = other_df[other_column].unique()
                    # Apply the cross-reference filter
                    filtered_df = filtered_df[filtered_df[conditions['column']].apply(lambda x: x in other_column_values)]
            else:
                if 'in' in conditions:
                    filter_values = [str(value).strip() for value in conditions['in']]
                    filtered_df = filtered_df[filtered_df[column].apply(lambda x: str(x).strip()).isin(filter_values)]
                if 'exact' in conditions:
                    exact_values = [str(value).strip() for value in conditions['exact']]
                    filtered_df = filtered_df[filtered_df[column].apply(lambda x: str(x).strip()).isin(exact_values)]
                if 'min' in conditions:
                    filtered_df = filtered_df[filtered_df[column] >= conditions['min']]
                if 'max' in conditions:
                    filtered_df = filtered_df[filtered_df[column] <= conditions['max']]
                if 'range' in conditions:
                    min_val, max_val = conditions['range']
                    filtered_df = filtered_df[(filtered_df[column] >= min_val) & (filtered_df[column] <= max_val)]
                if 'contains' in conditions:
                    expression = conditions['contains']
                    pattern = r'\b' + re.escape(expression) + r'\b'
                    
                    filtered_df = filtered_df[filtered_df[column].astype(str).str.contains(pattern, case=False, na=False, regex=True)]
        return filtered_df

    def group_and_aggregate(self):
        if self.df1 is not None:
            dialog = GroupAndAggregateDialog(self, self.df1.columns)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                selections = dialog.get_selections()
                
                # Unpack the selections based on what get_selections() actually returns
                if len(selections) == 3:
                    group_columns, value_column, agg_funcs = selections
                    agg_dict = {value_column: agg_funcs}
                elif len(selections) == 2:
                    group_columns, agg_dict = selections
                else:
                    QMessageBox.warning(self, "Invalid Selection", "Unexpected selection format.")
                    return

                if group_columns and agg_dict:
                    try:
                        # Create a copy of the DataFrame
                        df_grouped = self.df1.copy()

                        # Replace NaN with a placeholder in grouping columns
                        for col in group_columns:
                            df_grouped[col] = df_grouped[col].fillna('NaN')

                        # Identify columns not being aggregated
                        other_columns = [col for col in df_grouped.columns if col not in group_columns and col not in agg_dict]

                        # Perform the aggregation
                        agg_result = df_grouped.groupby(group_columns, dropna=False).agg(agg_dict)
                        
                        # Keep other columns without aggregation
                        other_result = df_grouped.groupby(group_columns, dropna=False)[other_columns].first()
                        
                        # Combine aggregated and non-aggregated results
                        result = pd.concat([agg_result, other_result], axis=1).reset_index()

                        # Rename only the aggregated columns
                        new_columns = []
                        for col in result.columns:
                            if isinstance(col, tuple):
                                if col[1] == '':
                                    new_columns.append(col[0])
                                else:
                                    new_columns.append(f"{col[0]}_{col[1]}")
                            else:
                                new_columns.append(col)
                        result.columns = new_columns

                        # Replace the placeholder back with NaN in group columns
                        for col in group_columns:
                            if col in result.columns:
                                result[col] = result[col].replace('NaN', np.nan)

                        # Store the aggregated table
                        self.aggregated_table = result
                        
                        # Display the aggregated data
                        agg_info = ', '.join([f"{col} ({', '.join(funcs) if isinstance(funcs, list) else funcs})" 
                                            for col, funcs in agg_dict.items()])
                        self.display_data(result, 'data1', f"Aggregated Data ({', '.join(group_columns)} - {agg_info})")
                        
                    except Exception as e:
                        QMessageBox.warning(self, "Aggregation Error", f"An error occurred during aggregation: {str(e)}")
                        print(f"Detailed error: {str(e)}")  # For debugging
                else:
                    QMessageBox.warning(self, "Invalid Selection", "Please select at least one group column and one aggregation function.")

    def clear_all_data(self):
        # Ask the user to confirm the data clearance
        response = QMessageBox.question(
            self, "Confirm Reset", "Are you sure you want to clear all data? This action cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No)

        if response == QMessageBox.StandardButton.Yes:
            # Reset all data frames
            self.df1 = None
            self.df2 = None
            self.original_df = None
            self.previous_merges = []  # Clear history of merges
            self.df2_names = []  # Clear history of dataset names for df2
            self.btn_revert_merge.setEnabled(False)  # Disable the revert button as there's nothing to revert

            # Reset UI components
            self.data_view1.setModel(None)  # Assuming data_view1 is for main data
            self.data_view2.setModel(None)  # Assuming data_view2 is for data to be merged
            self.tab_widget1.setTabText(0, "Data View")  # Reset tab title for data_view1
            self.tab_widget2.setTabText(0, "Data View")  # Reset tab title for data_view2

            # Clear selections and items in comboboxes and lists
            self.column_select1.clear()
            self.column_select2.clear()
            self.columns_to_add_selector.clear()
            self.columns_to_drop_selector.clear()
            self.merge_type_select.setCurrentIndex(0)  # Reset to default selection if needed
            self.data_history_combo.clear()  # Clear the data history combobox

            # Reset progress bar and status labels
            self.progress_bar.setValue(0)
            self.label_data_view1.setText("Main Data - No data loaded")
            self.label_data_view2.setText("Data for Merging - No data loaded")

            # Clear any active filters if applicable
            self.active_filters = {}
            if hasattr(self, 'filter_dialog') and self.filter_dialog.isVisible():
                self.filter_dialog.close()  # Close and reset the filter dialog if open

            # Clear statistics and plots
            self.overall_stats_label.setText("")  # Clear the overall statistics label
            if hasattr(self, 'stats_table_widget'):
                self.stats_table_widget.clearContents()  # Assuming stats_table_widget is a QTableWidget or similar
                self.stats_table_widget.setRowCount(0)
            if hasattr(self, 'plot_web_view'):
                self.plot_web_view.setHtml("")  # Clear any plots displayed in a QWebEngineView or similar

            QMessageBox.information(self, "Reset Successful", "All data has been cleared.")
        else:
            QMessageBox.information(self, "Reset Cancelled", "Data reset has been cancelled.")
                
    def update_map_data(self):
        if hasattr(self, 'map_view'):
            if not hasattr(self, 'original_df'):  # Backup is made only the first time this method is successfully called
                self.original_df = self.df1.copy()
            self.map_view.set_data_frame(self.df1)

# Define the stylesheet
stylesheet = """
    QMainWindow {
        background-color: #FAFAFA;
        font-family: 'Segoe UI', Arial, sans-serif;
        padding: 0px;  /* Changed from 10px to 0px */
    }

    QLabel {
        font-size: 14px;
        color: #2C3E50;
        margin-bottom: 5px;
    }

    QGroupBox {
        font-size: 16px;
        font-weight: bold;
        margin-top: 15px;
        padding: 10px;
        border: 1px solid #D6D6D6;
        border-radius: 5px;
    }

    QTableView {
        border: 1px solid #D6D6D6;
        font-size: 12px;
        gridline-color: #ECF0F1;
    }

    QTableView::item {
        padding: 5px;
    }

    QTableView::item:selected {
        background-color: #AED6F1;
    }

    QTableView QHeaderView::section {
        background-color: #EAF2F8;
        padding: 5px;
        border: 1px solid #D6D6D6;
        font-size: 14px;
        font-weight: bold;
        color: #5D6D7E;
    }

    QPushButton {
        background-color: #3498DB;
        color: white;
        border-radius: 5px;
        padding: 5px 15px;
        font-weight: bold;
        font-size: 13px;
        margin: 5px 0;
    }

    QPushButton:disabled {
        background-color: #95A5A6;
        color: #ECF0F1;
    }

    QPushButton:hover:!disabled {
        background-color: #2980B9;
    }

    QProgressBar {
        border: 1px solid #BDC3C7;
        border-radius: 5px;
        text-align: center;
    }

    QProgressBar::chunk {
        background-color: #2ECC71;
        width: 20px;
    }

    QComboBox, QListWidget {
        border: 1px solid #BDC3C7;
        border-radius: 5px;
        padding: 5px;
        background-color: #FFFFFF;
        selection-background-color: #D0ECE7;
        font-size: 13px;
        margin: 5px 0;
    }

    QComboBox::drop-down {
        border: 0px;
    }

    QInputDialog, QMessageBox {
        font-size: 13px;
    }

    QVBoxLayout, QHBoxLayout {
        spacing: 10px;
    }
"""

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(filename='application.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')

    # Initialize the application
    app = QApplication(sys.argv)
    window = DataMergerApp()

    # Apply the style sheet to the whole application
    app.setStyleSheet(stylesheet)
    # Show the main window
    window.show()

    # Start the application loop
    sys.exit(app.exec())


    
    
    