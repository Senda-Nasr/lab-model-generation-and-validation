import pandas as pd

#function to convert to lower case and remove spaces
def col_rename (dframe: pd.DataFrame) -> pd.DataFrame:
    """
    This function renames column names by removing spaces and converting to lower case
    Inputs: dframe of type pandas dataframe
    Outputs: returns the dataframe with the renamed columns
    """
    cols =[]
    for x in dframe.columns:
        if isinstance(x, str):
            cols.append(x.lower().replace(' ', '_'))
        else:
            cols.append(x)
    if 'st' in cols:
        index = cols.index('st')   
        cols[index]='state'

    dframe.columns=cols
    return dframe


#funnction to clean gender column and fills nulls with 'M','F', or 'U'
def clean_gender (dframe: pd.DataFrame)-> pd.DataFrame:
    """
    This function cleans the gender column
    It replaces all the different versions of female with "F"
    It replaces all the versions of male with "M"
    It replaces all other strings with "U" and it leaves all NaNs untouched
    
    Inputs: dframe of type pandas dataframe
    Outputs: returns the dataframe with a cleaned gender column
    """
    def apply_clean (original_gen):
        if isinstance(original_gen, str) and (original_gen[0].upper() in ['M', 'F']):
            original_gen=original_gen[0].upper()
        elif pd.isnull(original_gen):
            return original_gen
        else:
            original_gen='U'
        return original_gen

    dframe['gender']=dframe['gender'].apply(apply_clean)
    return dframe

    
#function to remove percentage sign
def convert_to_percentage (dframe: pd.DataFrame) -> pd.DataFrame:
    """
    This function cleans the customer lifetime value column
    It removes the percentage sign, and divides the number by 100
    
    Inputs: dframe of type pandas dataframe
    Outputs: returns the dataframe with the cleaned customer lifetime value column
    """
    dframe['customer_lifetime_value']=  dframe['customer_lifetime_value'].apply(lambda y: (float(y.rstrip('%')))/100 if isinstance(y,str) and '%' in y else y)
    return dframe


#funtion to drop rows with all null values
def remove_full_nulls (dframe: pd.DataFrame) -> pd.DataFrame:
    """
    """
    dframe= dframe.dropna(axis=0, how='all')
    return dframe


#extracting number of complaints from number of open complaints
def clean_complaints(dframe: pd.DataFrame) -> pd.DataFrame:
    """
    """
    dframe['number_of_open_complaints']=dframe['number_of_open_complaints'].apply(lambda x: int(x.split('/')[1]) if isinstance(x, str) else x)
    return dframe


#function to convert all numeric values to integers
def all_num_to_int (dframe: pd.DataFrame) -> pd.DataFrame:
    """
    """
    def conv_to_int (n) ->int:
        if pd.isnull(n):
            return n
        elif isinstance (n, (int,float)):
            return int(round(n))
        else:
            return n
        
    dfnew=dframe.applymap(conv_to_int)
    return dfnew



#function to replace state names
def standardize_states (dframe: pd.DataFrame) -> pd.DataFrame:
    """
    """
    dframe['state']=  dframe['state'].apply(lambda y: 'California' if isinstance(y,str) and y=='Cali' else y)
    #replacing AZ with Arizona:
    dframe['state']=  dframe['state'].apply(lambda y: 'Arizona' if isinstance(y,str) and y=='AZ' else y)
    #replacing WA with Washington:
    dframe['state']=  dframe['state'].apply(lambda y: 'Washington' if isinstance(y,str) and y=='WA' else y)

    return dframe


#function to clean education column
def clean_education (dframe: pd.DataFrame) -> pd.DataFrame:
    """
    """
    dframe['education']=  dframe['education'].apply(lambda y: 'Bachelor' if isinstance(y,str) and y=='Bachelors' else y)
    return dframe


#function to combine luxury categories in vehicle class
def combine_vehicle_class (dframe: pd.DataFrame) -> pd.DataFrame:
    """
    """
    dframe['vehicle_class']= dframe['vehicle_class'].apply(lambda y: 'Luxury' if isinstance(y,str) and y in ['Sports Car','Luxury SUV', 'Luxury Car'] else y)
    return dframe

def duplicate_checking (dframe: pd.DataFrame) -> pd.DataFrame:
    """
    """
    num_of_duplicates = dframe.duplicated().sum()
    if num_of_duplicates >0:
        dframe= dframe.drop_duplicates()
        return dframe
    else:
        return dframe
        
        

def clean_insurance_company_dataframe(dframe: pd.DataFrame) -> pd.DataFrame:
    '''
    This function will take a Pandas DataFrame and it will apply the previous functions in the library
    to clean some columns of the dataframe

    Inputs: 
    dframe: Pandas DataFrame

    Outputs:
    Cleaned dataframe DataFrame
    '''
    df= dframe.copy()
    df= remove_full_nulls(df)
    df= col_rename(df)
    df= clean_gender(df)
    df= convert_to_percentage(df)
    df= clean_complaints(df)
    df= all_num_to_int(df)
    df= standardize_states(df)
    df= clean_education(df)
    df= combine_vehicle_class(df)
    df= duplicate_checking(df)
    
    return df
