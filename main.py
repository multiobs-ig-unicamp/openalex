from download_openalex import *

if __name__ == "__main__":


    job_oa = prepare_job('works', 'work')

    #print(job_oa)
    job_oa = read_job_status('works')
    download_upload(job_oa)

    #print(job_oa)

 