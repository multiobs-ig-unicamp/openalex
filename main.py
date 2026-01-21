from download_openalex import *

if __name__ == "__main__":


    job_oa = prepare_job('authors', 'author')
    job_oa = read_job_status('authors')
    download_upload(job_oa)
 