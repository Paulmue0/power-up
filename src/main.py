
import os
import sys

from ev_approximation import ev, ev_cache
from helper import data_helper, user_interface_helper
from parkingspotfilter import parking
from redistricting import kmeans_batched, simple_split
from validation import validation
from visualization import viz

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))


"""
This module is the main entry point for the code
"""


def main():
    # Clear Console
    print("\033c", end='')
    print('\033[1m' + 'Step: 0: Setup' + '\033[0m')
    validation.create_directories()
    if validation.check_datasets():
        print('\033[1m' + 'Step: 1: Prepare Data' + '\033[0m')
        data_helper.run_data_preparation()
        print('\033[1m' + 'Step: 2: Electronic Vehicle Calculation' + '\033[0m')
        ev.run_ev_calculation()
        print('\033[1m' + 'Step: 3: Create Bubbles' + '\033[0m')
        bubble_algorithm = user_interface_helper.fancy_choice(
            "Choose The Algorithm You Want To Use.",
            "Simple Split (Most Efficient)",
            "K-Means (More Accurate but a lot more expensive)"
        )
        if bubble_algorithm == 1:
            print("Simple Split Is Being Executed")
            simple_split.run_splitting()
        elif bubble_algorithm == 2:
            print("KMeans Is Being Executed")
            print("Warning: This Can Take A Few Hours")
            kmeans_batched.run_kmeans_batched()
        print('\033[1m' + 'Step: 4: Map Bubbles To Parking Spaces' + '\033[0m')
        parking.run_parking_search(bubble_algorithm)
        print('\033[1m' + 'Step: 5: Visualization' + '\033[0m')
        viz.run_visualization()
        print('\033[1m' + 'Done' + '\033[0m')
        print(
            "A Visualization Can Be Found In The \033[3m'/maps'\033[0m  Folder")
        print("The List With The Points For The Charging Stations Can Be Found Here:")
        print("\033[3m'datasets/generated/charging_points.csv'\033[0m")


if __name__ == "__main__":
    sys.exit(main())
