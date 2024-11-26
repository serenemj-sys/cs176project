import pandas as pd

def cleanMergedData():
  # Get merged data from getData. paths are variable, need to adjust accordingly
  merged = getData(cali_path, melbourne_path, portugal_path)
  
  # Drop unnecessary columns
  columns_to_drop = ['stateId', 'countyId', 'cityId', 'pricePerSquareFoot', 'EnergyCertificate'
                      ,'street_address', 'District', 'town', 'city', 'District', 'total_rooms']
  merged.drop(columns_to_drop, axis=1, inplace=True)

  ## CLEANING COLUMN "HOME_TYPE"
  # Group all values into Land', 'House', 'Multi-Unit Housing', omitting those that don't fit within those 3 groups
  # 'Multi-Unit Housing' is the all-encompassing term for all Apartments/Duplexes/Units due to ambiguity in Melbourne data
  
  ## PORTUGAL HOME_TYPE CLEANUP
  # Dropping rows where 'home_type' is NOT 'House' or 'Duplex' or 'Apartment' or 'Land' from Portugal data
  portugal_hometype_drop = ['Farm', 'Store', 'Other - Residential', 'Building',
                            'Transfer of lease', 'Garage','Other - Commercial',
                            'Warehouse', 'Investment', 'Hotel', 'Office', 
                            'Storage','Industrial', 'Studio', 'Estate', 'Manor']
  merged.drop(merged[merged['home_type'].isin(portugal_hometype_drop)].index, inplace=True)
  
  # Mansions and Manors get merged with Houses, and 'Apartment' and 'Duplex' with 'Multi-Unit Housing'
  merged['home_type'] = merged['home_type'].replace('Mansion', 'House')
  merged['home_type'] = merged['home_type'].replace('Manor', 'House')
  merged['home_type'] = merged['home_type'].replace('Apartment', 'Multi-Unit Housing')
  merged['home_type'] = merged['home_type'].replace('Duplex', 'Multi-Unit Housing')
  
  ## CALIFORNIA HOME_TYPE CLEANUP
  """
  Single family homes and Townhouses are grouped with 'House', 
  Multi-Family Homes and Townhouse are grouped with 'Duplex'
  LOT is grouped with 'Land', CONDO and APARTMENT go under 'Apartment'
  """
  
  cali_repl = {
      'SINGLE_FAMILY': 'House',
      'TOWNHOUSE': 'House',
      'MULTI_FAMILY': 'Multi-Unit Housing',
      'LOT': 'Land',
      'CONDO': 'Multi-Unit Housing',
      'APARTMENT': 'Multi-Unit Housing'
  }
  merged['home_type'] = merged['home_type'].replace(cali_repl)
  
  
  ## MELBOURNE HOME_TYPE CLEANUP
  """
  h(houses) and t(townhouses) get merged with 'House'
  u(unit/duplex) gets merged with 'Apartment' as 
  No values of 'Land' in Melbourne data
  """
  
  merged['home_type'] = merged['home_type'].replace('h', 'House')
  merged['home_type'] = merged['home_type'].replace('t', 'House')
  merged['home_type'] = merged['home_type'].replace('u', 'Multi-Unit Housing')
  
  ## DROP NA
  merged.dropna(subset=['home_type'], inplace=True)





  ## PARKING CLEANUP
  
  # Drop Houses/Multi-Unit Housing with no bed/bath (Not housing - likely parking lots/towers)
  merged.drop(merged[((merged['home_type'] == 'House') | 
                  (merged['home_type'] == 'Multi-Unit Housing')) & 
                  (merged['bedrooms'] == 0) &
                  (merged['bathrooms'] == 0)
                   ].index, inplace=True)
  
  # Replace False values in garage_spaces to 0, and True values default to 1
  merged['garage_spaces'] = merged['garage_spaces'].replace(False, 0.0)
  merged['garage_spaces'] = merged['garage_spaces'].replace(True, 1.0)
  
  ## Merging parking_spaces and garage_spaces for clarity
  # Convert parking_spaces and garage_spaces to numeric values, preparing for merge
  merged['parking_spaces'] = pd.to_numeric(merged['parking_spaces'])
  merged['garage_spaces'] = pd.to_numeric(merged['garage_spaces'])
  
  # Add the two columns together, add back into parking_spaces
  merged['parking_spaces'] = merged['parking_spaces'] + merged['garage_spaces']
  
  # Drop garage_spaces column
  merged.drop('garage_spaces', axis=1, inplace=True)





  ## LEVELS CLEANUP: 3 parts - by Land, House, Multi-Unit Housing.
  ## Land is 0 for levels, levels for House means how many stories, 
  ## levels for Multi-Unit Housing signifies level the unit is located on- with 1 being on the ground
  
  # LAND CLEANUP
  # Drop values with home_type as 'Land' WITH bed/bath (Not land)
  merged.drop(merged[((merged['home_type'] == 'Land')) & 
                  (merged['bedrooms'] != 0) &
                  (merged['bathrooms'] != 0)
                   ].index, inplace=True)
  
  # Replace 'Land' values with levels anything other than 0 to 0
  land_repl = {'One Story' : 0,
               'Ground Floor' : 0,
               '2nd Floor' : 0,
               '3rd Floor' : 0
                }
  merged.loc[merged['home_type'] == 'Land', 'levels'] = merged.loc[merged['home_type'] == 'Land', 'levels'].replace(land_repl)
  
  # HOUSE CLEANUP
  
  # Drop unnecessary houses with irrelevant "levels" for clarity
  house_drop = ['Top Floor', 'Service Floor', 'Attic', 'Other-One',
                'Split Level', 'Multi-Level', 'Mezzanine', 'Basement', 
                'Basement Level', 'Triplex', 'Duplex', 'Multi/Split', 
                'Three or More Stories-Three Or More', 'Multi/Split-Tri-Level',
                'Three Or More-Split Level', 'Tri-Level-Two',
                'Multi/Split-Three Or More', 'One-Multi/Split',
                'One-Two-Three Or More', 'Two-Three Or More', 'Two Story-One', 'Other'
                ]
  merged.drop(merged[merged['levels'].isin(house_drop)].index, inplace=True)
  
  # Convert all 'levels' into numbers
  house_repl = {'One Story' : 1, 'Ground Floor' : 1,'One Story' : 1, 'Two Story' : 1, 'One' : 1, 'Two': 2, 'Three Or More' : 3,
                'One-Two' : 1, 'Three' : 3, 'Tri-Level' : 3, 'Three Or More-Multi/Split' : 3,
                'Three or More Stories' : 3, 'One Story-One' : 1, 'Two-Multi/Split' : 2,
                'Two Story-Two' : 2, 'Multi/Split-One' : 1, 'Multi/Split-Two' : 2, 'One-Two-Multi/Split' : 1,
                'One-Three Or More' : 1, '4+' : 4, 'Tri-Level-Three Or More' : 3, 'Three or More Stories-Two' : 1,
                'Three Or More-Two' : 1, 'Three or More Stories-One-Two' : 2, 'Two Story-Three Or More' : 2, 'Two-One' : 1, 
                '1st Floor' : 1, '2nd Floor' : 1, '3rd Floor' : 1, '0' : 1, 0 : 1
                }
  merged.loc[merged['home_type'] == 'House', 'levels'] = merged.loc[merged['home_type'] == 'House', 'levels'].replace(house_repl)
  
  
  # MULTI-UNIT HOUSING CLEANUP
  # Drop unnecessary units with irrelevant "levels" for clarity
  multi_drop = ['Two-Three Or More-Multi/Split', 'Multi/Split-Two', 'Two-Multi/Split', 'One Story-Two',
                'Tri-Level-Three Or More', 'Three Or More-Multi/Split', 'Two Story-Three Or More',
                 'Three or More Stories-Two', 'One-Three Or More', 'Two Story'
                ]
  merged.drop(merged[merged['levels'].isin(multi_drop)].index, inplace=True)
  
  # Convert all 'levels' into numbers
  multi_repl = {'One Story' : 1, 'One' : 1, 'Two' : 2, '0' : 1, 'Three Or More' : 3, 'Tri-Level' : 3,
         'Four' : 4, 'One Story-Three Or More' : 3, 'One Story-One' : 1, 'One-Two' : 2,
         'Two Story-Two' : 2, 'Three or More Stories-One' : 1,'Five or More' : 5,
         'Ground Floor' : 1, '1st Floor' : 1, 'Three or More Stories' : 1,
         '2nd Floor' : 1, '3rd Floor' : 1, '4th Floor' : 1, 'Above 10th Floor' : 10,
         '5th Floor' : 5, '6th Floor' : 6, '9th Floor' : 9, '7th Floor' : 7, '8th Floor' : 8
                }
  merged.loc[merged['home_type'] == 'Multi-Unit Housing', 'levels'] = merged.loc[merged['home_type'] == 'Multi-Unit Housing', 'levels'].replace(multi_repl)
  
  # Turn all values to numeric #s, making it easier to filter and visualize
  merged['levels'] = pd.to_numeric(merged['levels'])




  #For Filtering, you are free to modify/add/delete those as you see fit for visualization!

  # Filtering 1: Comparing Land prices from portugal and california
  land_prices = merged.loc[merged['frame'] != 'melbourne', ['price', 'TotalArea', 'frame']]
  
  # Filtering 2: Checking correlation between price and elevator availability in Multi-Unit Housing across all regions
  elev_aval = merged.loc[merged['home_type'] == 'Multi-Unit Housing', ['Elevator', 'price']]
  
  # Filtering 3: Checking correlation between # of parking spaces vs. price in each region
  park_aval = merged[['parking_spaces', 'price', 'frame']]
  
  # Filtering 4: Impact of year_built on price for 2-bedroom Multi-Unit Housing based in California
  year_built = merged.loc[(merged['frame'] == 'california') & 
              (merged['bedrooms'] == 2) &
              (merged['home_type'] == 'Multi-Unit Housing'),
              ['price', 'year_built']]
