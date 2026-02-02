from download_openalex import *

if __name__ == "__main__":


    job_oa = prepare_job('domains', 'domain',1)
    job_oa = read_job_status('domains')
    download_upload(job_oa)
 