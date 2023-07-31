import pyart
import fsspec
from radar_utils import valid_task
from get_data import get_radar
import matplotlib.pyplot as plt
from cartopy import crs as ccrs


def main():
    str_bucket = 's3://s3-radaresideam/'
    fs = fsspec.filesystem("s3", anon=True)
    files = fs.glob(f"{str_bucket}/l2_data/2018/09/12/Guaviare/GUA1809120*")
    for idx, file in enumerate(files[10:36]):
        radar = get_radar(file)
        if valid_task(radar):
            fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()})
            display = pyart.graph.RadarMapDisplay(radar)
            projection = ccrs.LambertConformal(
                central_latitude=radar.latitude["data"][0],
                central_longitude=radar.longitude["data"][0],
            )
            display.plot_ppi_map(
                "reflectivity",
                vmin=-10,
                vmax=50,
                projection=projection,
                ax=ax,
                lat_0=radar.latitude["data"][0],
                lon_0=radar.longitude["data"][0],
                cmap='pyart_ChaseSpectral'
            )

            gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                              linewidth=2, color='gray', alpha=0.5, linestyle='--')
            gl.xlabels_top = False
            gl.ylabels_right = False
            plt.savefig(f"../results/{idx}.png")
    print()


if __name__ == "__main__":
    main()
