import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

def combine_profit_loss(base_folder,output_path):
    all_data =[]
    companies = os.listdir(base_folder)
    logger.info(f"total companies found: {len(companies)}")

    for company in companies:
        company_path = os.path.join(base_folder,company)
        
        if os.path.isdir(company_path):

            for file in os.listdir(company_path):
                
                if "Yearly_Profit_Loss" in file:

                    file_path = os.path.join(company_path,file)

                    try:

                        df = pd.read_csv(file_path)
                        df['company_name']= company
                        all_data.append(df)

                    except Exception as e:

                        logger.warning(f"skipped {company}:{e}")  


    if all_data:

        combined = pd.concat(all_data, ignore_index = True)

        combined.to_csv(output_path,index=False)

        logger.info(f"combined file saved: {combined.shape[0]} rows")

        return combined

    else:
        logger.error("no data found")

        return None  

