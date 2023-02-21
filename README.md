# PowerUp

## Description
The number of electric vehicles (EVs) in Germany has risen steadily in recent years. 
In order to also further expand the public charging infrastructure required for this, 
the German government has set its expansion targets with the Charging Infrastructure Master Plan. 
According to this plan, there should be one million publicly accessible charging points by 2030. 
However, it is not only the number of charging points that is crucial, 
but also their location, in order to achieve a network that is as comprehensive as possible and adapted to regional use. 
The location search for public charging points is therefore a geoinformatics problem that will be addressed in this project.

Run instructions for the project:
```
cd power-up
python src/main.py
```

## Prerequisites
### Dependencies
Install all necessary requirements
```
pip install -r requirements.txt
```
### Folder Structure
To handle the datasets a extra folder is needed. The Folder should be called 'datasets' and be placed in the root of the project.
If there isn't such folder, then create it.
```
mkdir datasets
```
### Datasets
The following datasets are crucial and have to be downloaded and put into ./datasets
If any of the downloaded files dont have the name that we suggest. Please rename them.
1. List of Charging Stations
    + Name: Ladesaeulenregister.csv
    + https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister_CSV.csv;jsessionid=ABF392E556435F65E344E5658D1827E8?__blob=publicationFile&v=43
2. Vehicle Register Germany 
    + Name: fz27_202207.xlsx
    + https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ27/fz27_202207.xlsx;jsessionid=D0CEE3BE0DEBEA73D5540C4A206BDBAE.live11294?__blob=publicationFile&v=4

3. Census Data 100mx100m  -> ! has to be unpacked !
    + Name: Zensus_Bevoelkerung_100m-Gitter.csv
    + https://www.zensus2011.de/SharedDocs/Downloads/DE/Pressemitteilung/DemografischeGrunddaten/csv_Bevoelkerung_100m_Gitter.zip?__blob=publicationFile&v=2

4. Inspire data 100m -> ! has to be unpacked !
    + Name: /DE_Grid_ETRS89-LAEA_100m
    + https://daten.gdz.bkg.bund.de/produkte/sonstige/geogitter/aktuell/DE_Grid_ETRS89-LAEA_100m.csv.zip

5. Census Data for municipalities
    + Name: 1A_EinwohnerzahlGeschlecht.xls
    + https://www.zensus2011.de/SharedDocs/Downloads/DE/Pressemitteilung/DemografischeGrunddaten/1A_EinwohnerzahlGeschlecht.xls?__blob=publicationFile&v=2

6. List Of All Parking Spaces In Germany
    + Name: export.geojson
    + Should already be in the /datasets folder

