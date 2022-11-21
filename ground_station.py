import io
import sys
import threading
import time

from libs.kafka_libs import *
from jinja2 import Template

import folium
from PyQt5 import uic
from PyQt5.QtCore import pyqtSignal, QObject, QTimer
from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QtWebEngineWidgets import QWebEngineView

from uav_simulator import airplane


class Window(QMainWindow):
    coordinate_changed = pyqtSignal(float, float)
    coordinate_changed_label = pyqtSignal(float, float)
    fuel_changed = pyqtSignal(float)
    state_changed_label = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.producer = get_producer()
        self.coordinate_changed.connect(self.add_marker)
        self.coordinate_changed_label.connect(self.label_update)
        self.fuel_changed.connect(self.update_fuel)
        self.state_changed_label.connect(self.update_state)
        uic.loadUi("/home/simulator/backend/formCylindricalProyection.ui",self)
        coordinate = (40.4927751, -3.5933761)
        self.map = folium.Map(
            zoom_start=15, location=coordinate, control_scale=True, tiles=None
        )
        folium.raster_layers.TileLayer(
            tiles="http://mt1.google.com/vt/lyrs=m&h1=p1Z&x={x}&y={y}&z={z}",
            name="Standard Roadmap",
            attr="Google Map",
        ).add_to(self.map)
        folium.raster_layers.TileLayer(
            tiles="http://mt1.google.com/vt/lyrs=s&h1=p1Z&x={x}&y={y}&z={z}",
            name="Satellite Only",
            attr="Google Map",
        ).add_to(self.map)
        folium.raster_layers.TileLayer(
            tiles="http://mt1.google.com/vt/lyrs=y&h1=p1Z&x={x}&y={y}&z={z}",
            name="Hybrid",
            attr="Google Map",
        ).add_to(self.map)
        folium.LayerControl().add_to(self.map)
        folium.Marker(coordinate).add_to(self.map)
        data = io.BytesIO()
        self.map.save(data, close_file=False)
        self.map_view = QWebEngineView()
        self.map_view.setHtml(data.getvalue().decode())
        self.layaout_planner.addWidget(self.map_view)
        self.plane_on_button.clicked.connect(self.on_plane)
        self.lift_off_button.clicked.connect(self.handleButton)
        self.pushButton_both.clicked.connect(self.curve_changed)
        self.to_land_button.clicked.connect(self.init_to_land)
        self.stop = False
        self.plane = airplane()

    def init_to_land(self):
        send_msg(self.producer, "aterrizaje", "aterrizaje avion", 0)
    #     thread_plane = threading.Thread(target=self.to_land)
    #     thread_plane.daemon = True
    #     thread_plane.start()
    #
    # def to_land(self):
    #     self.stop = True
    #     self.sim.to_land()

    def on_plane(self):
        send_msg(self.producer, "init_plane", "on plane", 0)
        thread_state = threading.Thread(target=self.timer_plane_state)
        thread_state.daemon = True
        thread_state.start()

    def timer_plane_state(self):
        self.consumer_state = get_consumer(["state_plane"])
        self.consumer_fuel = get_consumer(["fuel"])
        while True:
            self.plane_states()
            if not self.stop:
                self.plane_fuel()
            time.sleep(0.5)

    def update_state(self, state):
        self.label_lat_3.setText(state)

    def update_fuel(self, fuel):
        self.label_fuel.setText(str(round(float(fuel), 4)) + " %")

    def plane_states(self):
        try:
            list_answer = read_msg(self.consumer_state)
            for answer in list_answer:
                self.state_changed_label.emit(answer.value["state"])
        except Exception as e:
            print("EEROR" + str(e))

    def plane_fuel(self):
        try:
            list_answer = read_msg(self.consumer_fuel)
            for answer in list_answer:
                self.fuel_changed.emit(answer.value["FUEL"])
        except Exception as e:
            print("EEROR" + str(e))


    def handleButton(self):
        send_msg(self.producer, "init_lift", "init_lift off", 0)
        # thread_plane = threading.Thread(target=self.init_plane)
        # thread_plane.daemon = True
        # thread_plane.start()
        thead_point = threading.Thread(target=self.time_thread)
        thead_point.daemon = True
        thead_point.start()

    def init_plane(self):
        self.sim.init_lift_off()

    def time_thread(self):
        while True:
            self.update_marker()
            time.sleep(0.5)

    def update_marker(self):
        self.consumer = get_consumer(["aterrizaje", "coord"])
        try:
            list_answer = read_msg(self.consumer)
            for answer in list_answer:
                self.coordinate_changed.emit(answer.value["LAT"], answer.value["LON"])
                self.coordinate_changed_label.emit(answer.value["LAT"], answer.value["LON"])
        except Exception as e:
            print("EEROR" + str(e))


    def curve_changed(self):
        grades = float(self.lineEdit_target_elevation.text())
        msg = {"request": "curve", "grades": grades}
        send_msg(self.producer, "curve", msg, 0)

    def label_update(self, lat, lon):
        self.label_lat.setText(str(round(float(lat), 6)))
        self.label_lon.setText(str(round(float(lon), 6)))

    def add_marker(self, latitude, longitude):
        js = Template(
            """
        L.marker([{{latitude}}, {{longitude}}] )
            .addTo({{map}});
        L.circleMarker(
            [{{latitude}}, {{longitude}}], {
                "bubblingMouseEvents": true,
                "color": "#3388ff",
                "dashArray": null,
                "dashOffset": null,
                "fill": false,
                "fillColor": "#3388ff",
                "fillOpacity": 0.2,
                "fillRule": "evenodd",
                "lineCap": "round",
                "lineJoin": "round",
                "opacity": 1.0,
                "radius": 2,
                "stroke": true,
                "weight": 5
            }
        ).addTo({{map}});
        """
        ).render(map=self.map.get_name(), latitude=latitude, longitude=longitude)
        self.map_view.page().runJavaScript(js)



def main():
    app = QApplication(sys.argv)

    window = Window()
    window.showMaximized()



    sys.exit(app.exec())


if __name__ == "__main__":
    main()