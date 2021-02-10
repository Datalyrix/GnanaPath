import subprocess

def   job_execute_wrapper():

    process = subprocess.Popen(['sh', 'runjob.sh'],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE) 
    (ostr, estr) = process.communicate()
    print(ostr)
    #print(estr)


if __name__ == "__main__":
     
    ret= job_execute_wrapper()
    print('JOB IS completed ');
