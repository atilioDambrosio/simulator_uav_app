import threading
import time
from functools import partial
from libs.kafka_libs import *

class airplane():
    def __init__(self):
        self.lat = 40.4927751
        self.lon = -3.5933761
        # self.lon = -0.5
        self.vector_point = 0
        self.lon_point = 0
        self.first = True
        self.pos = 0
        self.producer = get_producer()
        # create_topics(["curve","aterrizaje","coord"])
        self.consumer = get_consumer(["aterrizaje", "curve", "init_plane", "init_lift"])
        self.velocity_grade = 4.5
        self.fuel = 100.0
        self.init_dict_functions()

    def init_dict_functions(self):
        self.dict_functions_topic = {
            "aterrizaje": partial(self.to_land),
            "init_plane": partial(self.init_on_plane),
            "init_lift": partial(self.init_lift_off),
            "curve": partial(self.curve_plane, curve=None)}
        kafka_controller = threading.Thread(target=self.read_messages)
        kafka_controller.daemon = True
        kafka_controller.start()

    def read_messages(self):
        while True:
            list_message = read_msg(self.consumer)
            try:
                for message in list_message:
                    if message.topic == "curve":
                        self.dict_functions_topic[message.topic](curve=message.value["grades"])
                    else:
                        self.dict_functions_topic[message.topic]()
            except Exception as e:
                print("ERROR", str(e))

    def init_on_plane(self):
        thread_fuel = threading.Thread(target=self.timer_on_plane)
        thread_fuel.start()

    def timer_on_plane(self):
        message = {"state": "INIT PLANE", "Time": str(time.time())}
        print(message)
        send_msg(self.producer, "state_plane", message, 0)
        while True:
            self.on_plane()
            time.sleep(1)

    def on_plane(self):
        message = {"FUEL": self.fuel, "Time": str(time.time())}
        print(message)
        send_msg(self.producer, "fuel", message, 0)
        self.fuel = self.fuel - 0.001

    def to_land(self):
        message = {"state": "to land", "Time": str(time.time())}
        send_msg(self.producer, "state_plane", message, 0)
        self.vector_point = self.get_vector_lift_off()
        self.plane = False
        for point in reversed(self.vector_point):
            self.lat = self.lat + point
            message = {"LAT": self.lat, "LON": self.lon, "Time": str(time.time())}
            print(message)
            send_msg(self.producer, "coord", message, 0)
            time.sleep(1)
            # self.lat_point = point

        message = {"state": "Finish/OFF", "Time": str(time.time())}
        send_msg(self.producer, "state_plane", message, 0)


    def init_lift_off(self):
        message = {"state": "Lift off", "Time": str(time.time())}
        send_msg(self.producer, "state_plane", message, 0)
        self.vector_point = self.get_vector_lift_off()
        for point in self.vector_point:
            self.get_cuadrante()
            self.lat = self.lat + point
            message = {"LAT": self.lat, "LON": self.lon, "Time": str(time.time())}
            print(message)
            send_msg(self.producer, "coord", message, 0)
            time.sleep(1)
            self.lat_point = point
        self.vel_grade = self.lat_point/20
        handlecon_thread = threading.Thread(target=self.init_fly)
        handlecon_thread.start()


    def init_fly(self):
        message = {"state": "Init fly", "Time": str(time.time())}
        send_msg(self.producer, "state_plane", message, 0)
        self.plane = True
        print("init fly")
        while self.plane:
            self.get_cuadrante()
            self.lat = self.lat + self.lat_point*self.direc_lat
            self.lon = self.lon + self.lon_point*self.direct_lon
            message = {"LAT": self.lat, "LON": self.lon, "Time": str(time.time())}
            print(message)
            send_msg(self.producer, "coord", message, 0)
            time.sleep(1)

    def get_cuadrante(self):
        if self.pos < 0:
            self.pos = self.pos + 360
        if self.pos > 360:
            self.pos = self.pos - 360
        if 0 <= self.pos <= 90:
            self.symbol_lat = -1
            self.symbol_lon = 1
            self.direc_lat = 1
            self.direct_lon = 1
        elif 90 < self.pos <= 180:
            self.symbol_lat = 1
            self.symbol_lon = -1
            self.direc_lat = -1
            self.direct_lon = 1
        elif 180 < self.pos <= 270:
            self.symbol_lat = -1
            self.symbol_lon = 1
            self.direc_lat = -1
            self.direct_lon = -1
        elif 270 < self.pos <= 360:
            self.symbol_lat = 1
            self.symbol_lon = -1
            self.direc_lat = 1
            self.direct_lon = -1

    def curve_plane(self, curve):
        message = {"state": "Init curve", "Time": str(time.time())}
        send_msg(self.producer, "state_plane", message, 0)
        for i in range(int(curve/self.velocity_grade)):
            self.first = False
            self.pos = self.pos + self.velocity_grade
            self.get_cuadrante()
            self.lat_point = (self.lat_point + self.vel_grade*self.symbol_lat)
            self.lon_point = (self.lon_point + self.vel_grade*self.symbol_lon)
            time.sleep(1)
        message = {"state": "Finish curve", "Time": str(time.time())}
        send_msg(self.producer, "state_plane", message, 0)

    def get_vector_lift_off(self):
        vector_point = []
        for i in range(16):
            vi_ = self.get_velocity(i)
            vector_point.append(self.get_point(vi_))
        return vector_point

    def calculate_distance(self, vi, time, a_km):
        a = self.get_meter_seconds(a_km)
        distance = vi * time + (a * time * time) / 2
        return distance

    def get_velocity(self, time):
        if time == 0:
            return 0
        else:
            return 5.56 * time

    def get_point(self, velocity):
        point = (velocity * 0.00075) / 83.34
        return point

    def get_meter_seconds(self, km):
        return (km * 1000) / (60 * 60)

if __name__ == "__main__":
    sim = airplane()
    sim.init_lift_off()
