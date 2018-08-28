'''
setup dataset for ATLAS

background (TODO: complete the background):
- _input_dataset_file_preparation prepares all the information so that it can be reused in all the other steps
- jobs with missing files have file.status = pending and have job.dispatchdblock set.
  For these jobs the function setupSource + _subscribeDistpatchDB take care of the input subscriptions
- jobs with existing files have file.status = ready and job.dispatchdblock = None.
  For these jobs the function _makeDisDatasetsForExistingfiles takes care of the input subscriptions (like a pinning)
- in order to optimize the interactions with Rucio (i.e. not to generate datasets with one file), several jobs should
  share the same dispatch block
'''

import re
import sys
import time
import uuid
import datetime
import commands
import traceback
import ErrorCode

from taskbuffer.DatasetSpec import DatasetSpec
from taskbuffer import EventServiceUtils
from brokerage.SiteMapper import SiteMapper
import brokerage.broker
import DataServiceUtils
from SetupperPluginBase import SetupperPluginBase
from DDM import rucioAPI

from rucio.common.exception import DataIdentifierNotFound

class SetupperAtlasPlugin (SetupperPluginBase):
    # constructor
    def __init__(self, taskBuffer, jobs, logger, **params):
        # defaults
        defaultMap = {'resubmit': False,
                      'pandaDDM': False,
                      'ddmAttempt': 0,
                      'resetLocation': False,
                      'useNativeDQ2': True,
                     }
        SetupperPluginBase.__init__(self, taskBuffer, jobs, logger, params, defaultMap)
        # VUIDs of dispatchDBlocks
        self.vuidMap = {}
        # file list by dispatch dataset
        self.dispatch_file_map = {}
        # site mapper
        self.siteMapper = None
        # location map
        self.replicaMap  = {}
        # all replica locations
        self.allReplicaMap = {}
        # replica map for special brokerage
        self.replicaMapForBroker = {}
        # available files at T2
        self.availableLFNsInT2 = {}
        # list of missing datasets
        self.missingDatasetList = {}
        # lfn ds map
        self.lfnDatasetMap = {}
        # missing files at T1
        self.missingFilesInT1 = {}

    def __call_retry(self, call, *args, **kwargs):
        """
        Wrapper to make remote calls reliable
        """

        max_attempts = 3
        sleep_time = 10

        for i in xrange(1, max_attempts + 1):
            try:
                return True, call(*args, **kwargs)
            except:
                # Log the error
                err_type, err_value = sys.exc_info()[:2]
                err_msg = "Call to {0} failed with {1}:{2}".format(call, err_type, err_value)
                self.logger.debug(err_msg)

                if i < max_attempts:
                    # sleep and retry
                    time.sleep(sleep_time)
                else:
                    # retried enough, return the error message
                    return False, err_msg
        
    # main
    def run(self):
        try:
            self.logger.debug('start run()')
            self._memoryCheck()
            bunch_tag = ''
            time_start = datetime.datetime.utcnow()

            if self.jobs is not None and len(self.jobs) > 0:
                bunch_tag = 'PandaID:{0} type:{1} taskID:{2} pType={3}'.format(self.jobs[0].PandaID,
                                                                               self.jobs[0].prodSourceLabel,
                                                                               self.jobs[0].taskID,
                                                                               self.jobs[0].processingType)
                self.logger.debug(bunch_tag)

            # instantiate site mapper
            self.siteMapper = SiteMapper(self.taskBuffer)

            # gather all the information
            self._input_dataset_file_preparation()

            # invoke brokerage
            self.logger.debug('brokerSchedule')
            self._memoryCheck()
            brokerage.broker.schedule(self.jobs,self.taskBuffer,self.siteMapper,
                                      replicaMap=self.replicaMapForBroker,
                                      t2FilesMap=self.availableLFNsInT2)

            # remove waiting jobs
            self.removeWaitingJobs()

            # setup dispatch dataset
            self.logger.debug('setupSource')
            self._memoryCheck()
            self._setupSource()
            self._memoryCheck()

            # sort jobs by site so that larger subs are created in the next step
            if self.jobs != [] and self.jobs[0].prodSourceLabel in ['managed','test']:
                tmpJobMap = {}
                for tmpJob in self.jobs:
                    # add site
                    tmpJobMap.setdefault(tmpJob.computingSite, [])
                    # add job
                    tmpJobMap[tmpJob.computingSite].append(tmpJob)

                # redo the job list
                tmpJobList = []
                for tmpSiteKey in tmpJobMap.keys():
                    tmpJobList += tmpJobMap[tmpSiteKey]

                # set new list
                self.jobs = tmpJobList

            # create output datasets and assign destination
            if self.jobs != [] and self.jobs[0].prodSourceLabel in ['managed', 'test']:
                # count the number of jobs per _dis
                iBunch = 0
                prevDisDsName = None
                nJobsPerDisList = []
                for tmpJob in self.jobs:
                    if prevDisDsName != None and prevDisDsName != tmpJob.dispatchDBlock:
                        nJobsPerDisList.append(iBunch)
                        iBunch = 0
                    # increment
                    iBunch += 1
                    # set _dis name
                    prevDisDsName = tmpJob.dispatchDBlock
                # remaining
                if iBunch != 0:
                    nJobsPerDisList.append(iBunch)

                # split sub datasets
                iBunch = 0
                nBunchMax = 50
                tmpIndexJob = 0
                for nJobsPerDis in nJobsPerDisList:
                    # check _dis boundary so that the same _dis doesn't contribute to many _subs
                    if iBunch + nJobsPerDis > nBunchMax:
                        if iBunch != 0:
                            self._setupDestination(startIdx=tmpIndexJob, nJobsInLoop=iBunch)
                            tmpIndexJob += iBunch
                            iBunch = 0
                    # increment
                    iBunch += nJobsPerDis
                # remaining
                if iBunch != 0:
                    self._setupDestination(startIdx=tmpIndexJob, nJobsInLoop=iBunch)
            else:
                # at a burst
                self._setupDestination()

            # make dis datasets for existing files
            self._memoryCheck()
            self._makeDisDatasetsForExistingfiles()
            self._memoryCheck()

            # setup jumbo jobs
            self._setupJumbojobs()
            self._memoryCheck()
            reg_time = datetime.datetime.utcnow() - time_start
            self.logger.debug('{0} took {1}sec'.format(bunch_tag, reg_time.seconds))
            self.logger.debug('end run()')
        except:
            err_type, err_value = sys.exc_info()[:2]
            err_str = "run() : %s %s" % (err_type, err_value)
            err_str.strip()
            err_str += traceback.format_exc()
            self.logger.error(err_str)

    def postRun(self):
        """
        Post run
        :return:
        """
        try:
            self.logger.debug('start postRun()')
            self._memoryCheck()

            # subscribe sites distpatchDBlocks. this must be the last method
            self.logger.debug('subscribeDistpatchDB')
            self._subscribeDistpatchDB()
            self._memoryCheck()

            # dynamic data placement for analysis jobs
            self.logger.debug('dynamicDataPlacement')
            self._dynamicDataPlacement()
            self._memoryCheck()

            self.logger.debug('end postRun()')
        except:
            errtype, errvalue = sys.exc_info()[:2]
            self.logger.error("postRun() : %s %s" % (errtype, errvalue))

    def _setupSource(self):
        """
        This function creates the INPUT/DISPATCH datasets, resolves the files, and registers them in Rucio and PanDA database.
        There are no subscriptions made for the dispatch blocks yet
        :return:
        """

        file_list = {} # file details for each dispatch block
        input_ds_list = [] # list of input datasets
        input_ds_errors = {} # errors for each input dataset
        dispatch_ds_errors = {} # errors for each dispatch block
        ds_to_task_map = {} # dataset to task ID map

        # extract prodDBlock
        for job in self.jobs:

            # ignore failed jobs
            if job.jobStatus in ['failed', 'cancelled'] or job.isCancelled():
                continue

            # list input datasets and make the error map
            if job.prodDBlock != 'NULL' and (not self.pandaDDM) and job.prodSourceLabel not in ['user','panda']:
                # get VUID and record prodDBlock into DB
                if job.prodDBlock not in input_ds_errors:
                    self.logger.debug('listDatasets {0}'.format(job.prodDBlock))
                    input_ds_errors[job.prodDBlock] = ''
                    for iDDMTry in range(3):
                        newOut, errMsg = rucioAPI.listDatasets(job.prodDBlock)
                        if newOut is None:
                            time.sleep(10)
                        else:
                            break
                    if newOut is None:
                        input_ds_errors[job.prodDBlock] = "Setupper._setupSource() could not get VUID of prodDBlock with {0}".format(errMsg)
                        self.logger.error(errMsg)                                            
                    else:
                        self.logger.debug(newOut)
                        try:
                            vuids = newOut[job.prodDBlock]['vuids']
                            nfiles = 0
                            # dataset spec
                            ds = DatasetSpec()
                            ds.vuid = vuids[0]
                            ds.name = job.prodDBlock
                            ds.type = 'input'
                            ds.status = 'completed'
                            ds.numberfiles  = nfiles
                            ds.currentfiles = nfiles
                            input_ds_list.append(ds)
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            self.logger.error("_setupSource() : %s %s" % (errtype,errvalue))
                            input_ds_errors[job.prodDBlock] = "Setupper._setupSource() could not decode VUID of prodDBlock"
                # error
                if input_ds_errors[job.prodDBlock] != '':
                    job.jobStatus = 'failed'
                    job.ddmErrorCode = ErrorCode.EC_Setupper
                    job.ddmErrorDiag = input_ds_errors[job.prodDBlock]
                    continue

            # prepare dispatch datasets: retrieve file information and replica locations
            if job.dispatchDBlock != 'NULL':

                # file_list
                if not file_list.has_key(job.dispatchDBlock):
                    file_list[job.dispatchDBlock] = {'lfns':[], 'guids':[], 'fsizes':[], 'md5sums':[], 'chksums':[]}
                    ds_to_task_map[job.dispatchDBlock] = job.jediTaskID

                # collect LFN and GUID
                for file in job.Files:
                    if file.type == 'input' and file.status == 'pending':

                        # fill file information
                        tmpLFN = '{0}:{1}'.format(file.scope,file.lfn)
                        if not tmpLFN in file_list[job.dispatchDBlock]['lfns']:
                            file_list[job.dispatchDBlock]['lfns'].append(tmpLFN)
                            file_list[job.dispatchDBlock]['guids'].append(file.GUID)
                            if file.fsize in ['NULL',0]:
                                file_list[job.dispatchDBlock]['fsizes'].append(None)
                            else:
                                file_list[job.dispatchDBlock]['fsizes'].append(long(file.fsize))
                            if file.md5sum in ['NULL','']:
                                file_list[job.dispatchDBlock]['md5sums'].append(None)
                            elif file.md5sum.startswith("md5:"):
                                file_list[job.dispatchDBlock]['md5sums'].append(file.md5sum)                      
                            else:
                                file_list[job.dispatchDBlock]['md5sums'].append("md5:%s" % file.md5sum)                      
                            if file.checksum in ['NULL','']:
                                file_list[job.dispatchDBlock]['chksums'].append(None)
                            else:
                                file_list[job.dispatchDBlock]['chksums'].append(file.checksum)

                        # get replica locations
                        self.replicaMap.setdefault(job.dispatchDBlock, {})
                        if file.dataset not in self.allReplicaMap:

                            if file.dataset.endswith('/'):
                                # container
                                status, out = self.getListDatasetReplicasInContainer(file.dataset)
                            else:
                                # dataset
                                status, out = self.getListDatasetReplicas(file.dataset, False)

                            if status != 0 or out.startswith('Error'):
                                self.logger.error(out)
                                dispatch_ds_errors[job.dispatchDBlock] = 'could not get locations for %s' % file.dataset
                                self.logger.error(dispatch_ds_errors[job.dispatchDBlock])
                            else:
                                self.logger.debug(out)
                                tmp_replicas_sites = {}
                                try:
                                    # convert res to map
                                    exec "tmp_replicas_sites = %s" % out
                                    self.allReplicaMap[file.dataset] = tmp_replicas_sites
                                except:
                                    dispatch_ds_errors[job.dispatchDBlock] = 'could not convert HTTP-res to replica map for %s' % file.dataset
                                    self.logger.error(dispatch_ds_errors[job.dispatchDBlock])
                                    self.logger.error(out)

                        if file.dataset in self.allReplicaMap:
                            self.replicaMap[job.dispatchDBlock][file.dataset] = self.allReplicaMap[file.dataset]

        # register dispatch dataset
        dispList = []
        for dispatch_block in file_list.keys():
            # ignore empty dataset
            if len(file_list[dispatch_block]['lfns']) == 0:
                continue
            # use DDM/Rucio
            if (not self.pandaDDM) and job.prodSourceLabel != 'ddm':

                # register dispatch dataset
                self.dispatch_file_map[dispatch_block] = file_list[dispatch_block]
                disFiles = file_list[dispatch_block]
                metadata = {'hidden': True,
                            'purge_replicas': 0}
                if dispatch_block in ds_to_task_map and ds_to_task_map[dispatch_block] not in ['NULL', 0]:
                    metadata['task_id'] = str(ds_to_task_map[dispatch_block])

                # register dataset in Rucio
                self.logger.debug('registerDataset {ds} {meta}'.format(ds=dispatch_block, meta=str(metadata)))
                errStr = ''
                status, out = self.__call_retry(rucioAPI.registerDataset, dispatch_block, disFiles['lfns'],
                                                disFiles['guids'], disFiles['fsizes'], disFiles['chksums'],
                                                lifetime=7, scope='panda', metadata=metadata)
                if not status:
                    # registration failed
                    dispatch_ds_errors[dispatch_block] = "Setupper._setupSource() could not register dispatchDBlock with {0}".format(out)
                    continue

                self.logger.debug(out)
                new_out = out

                # freeze dispatch dataset
                self.logger.debug('closeDataset {0}'.format(dispatch_block))
                status, out = self.__call_retry(rucioAPI.closeDataset, dispatch_block)
                if not status:
                    self.logger.error(out)
                    dispatch_ds_errors[dispatch_block] = "Setupper._setupSource() could not freeze dispatchDBlock with {0}".format(out)
                    continue
            else:
                # use PandaDDM
                self.dispatch_file_map[dispatch_block] = file_list[dispatch_block]
                # create a fake vuid
                new_out = {'vuid': str(uuid.uuid4())}

            # get VUID
            try:
                vuid = new_out['vuid']
                # datasetspec.currentfiles is used to count the number of failed jobs
                ds = DatasetSpec()
                ds.vuid = vuid
                ds.name = dispatch_block
                ds.type = 'dispatch'
                ds.status = 'defined'
                ds.numberfiles  = len(file_list[dispatch_block]['lfns'])
                try:
                    ds.currentfiles = long(sum(filter(None,file_list[dispatch_block]['fsizes']))/1024/1024)
                except:
                    ds.currentfiles = 0

                dispList.append(ds)
                self.vuidMap[ds.name] = ds.vuid
            except:
                errtype, errvalue = sys.exc_info()[:2]
                self.logger.error("_setupSource() : %s %s" % (errtype, errvalue))
                dispatch_ds_errors[dispatch_block] = "Setupper._setupSource() could not decode VUID dispatchDBlock"

        # insert datasets to DB
        self.taskBuffer.insertDatasets(input_ds_list+dispList)
        # job status
        for job in self.jobs:
            if dispatch_ds_errors.has_key(job.dispatchDBlock) and dispatch_ds_errors[job.dispatchDBlock] != '':
                job.jobStatus = 'failed'
                job.ddmErrorCode = ErrorCode.EC_Setupper                
                job.ddmErrorDiag = dispatch_ds_errors[job.dispatchDBlock]

        # delete explicitly some huge variables
        del file_list
        del input_ds_list
        del input_ds_errors

    def _setupDestination(self, startIdx=-1, nJobsInLoop=50):
        """
        Create OUTPUT/SUB datasets in the repository and assign destination
        :param startIdx:
        :param nJobsInLoop:
        :return:
        """

        self.logger.debug('setupDestination idx:{0} n:{1}'.format(startIdx, nJobsInLoop))
        dest_error = {}
        dataset_map = {}
        new_name_map = {}
        sn_retrieved_datasets = []

        # take the jobs specified in the indices
        if startIdx == -1:
            job_list = self.jobs
        else:
            job_list = self.jobs[startIdx:startIdx+nJobsInLoop]

        for job in job_list:
            self.logger.debug('job computingsite:{0} destinationse:{1}'.format(job.computingSite, job.destinationSE))

            # ignore failed jobs
            if job.jobStatus in ['failed', 'cancelled'] or job.isCancelled():
                continue

            zip_file_map = job.getZipFileMap()
            for file in job.Files:
                self.logger.debug('file destinationDBlock:{0} destinationSE:{1} destinationDBlockToken:{2}'.format(file.destinationDBlock, file.destinationSE, file.destinationDBlockToken))
                # ignore input files
                if file.type in ['input', 'pseudo_input']:
                    continue
                # don't touch with outDS for unmerge jobs
                if job.prodSourceLabel == 'panda' and job.processingType == 'unmerge' and file.type != 'log':
                    continue

                # extract destinationDBlock, destinationSE and computingSite
                dest = (file.destinationDBlock, file.destinationSE, job.computingSite, file.destinationDBlockToken)
                if not dest_error.has_key(dest):
                    dest_error[dest] = ''
                    original_name = ''
                    if (job.prodSourceLabel == 'panda') or (job.prodSourceLabel in ['ptest','rc_test'] and \
                                                            job.processingType in ['pathena','prun','gangarobot-rctest']):
                        # keep original name
                        ds_name_list = [file.destinationDBlock]
                    else:
                        # set freshness to avoid redundant DB lookup
                        definedFreshFlag = None
                        if file.destinationDBlock in sn_retrieved_datasets:
                            # already checked
                            definedFreshFlag = False
                        elif job.prodSourceLabel in ['user','test','prod_test']:
                            # user or test datasets are always fresh in DB
                            definedFreshFlag = True

                        # get serial number
                        sn, freshFlag = self.taskBuffer.getSerialNumber(file.destinationDBlock, definedFreshFlag)
                        if sn == -1:
                            dest_error[dest] = "Setupper._setupDestination() could not get serial num for {0}".format(file.destinationDBlock)
                            continue
                        if not file.destinationDBlock in sn_retrieved_datasets:
                            sn_retrieved_datasets.append(file.destinationDBlock)

                        # new dataset name
                        new_name_map[dest] = "{0}_sub0{1}".format(file.destinationDBlock, sn)
                        if freshFlag or self.resetLocation:
                            # register original dataset and new dataset
                            ds_name_list = [file.destinationDBlock, new_name_map[dest]]
                            original_name = file.destinationDBlock
                        else:
                            # register new dataset only
                            ds_name_list = [new_name_map[dest]]

                    # create dataset
                    for ds_name in ds_name_list:
                        computing_site = job.computingSite
                        if ds_name == original_name and not ds_name.startswith('panda.um.'):
                            # for original dataset
                            computing_site = file.destinationSE

                        newVUID = None
                        # skip dataset registraton for zip files
                        if file.lfn in zip_file_map:
                            newVUID = str(uuid.uuid4())
                        elif (not self.pandaDDM) and (job.prodSourceLabel != 'ddm') and (job.destinationSE != 'local'):
                            # get src and dest DDM conversion is needed for unknown sites
                            if job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(computing_site):
                                # DQ2 ID was set by using --destSE for analysis job to transfer output
                                tmpSrcDDM = self.siteMapper.getSite(job.computingSite).ddm_output
                            else:                            
                                tmpSrcDDM = self.siteMapper.getSite(computing_site).ddm_output

                            if job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(file.destinationSE):
                                # DQ2 ID was set by using --destSE for analysis job to transfer output 
                                tmpDstDDM = tmpSrcDDM
                            elif DataServiceUtils.getDestinationSE(file.destinationDBlockToken) != None:
                                # destination is specified
                                tmpDstDDM = DataServiceUtils.getDestinationSE(file.destinationDBlockToken)
                            else:
                                tmpDstDDM = self.siteMapper.getSite(file.destinationSE).ddm_output
                                
                            # skip registration for _sub when src=dest
                            if ((tmpSrcDDM == tmpDstDDM and not EventServiceUtils.isMergeAtOS(job.specialHandling)) \
                                    or DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) != None) \
                                    and ds_name != original_name and re.search('_sub\d+$', ds_name) != None:
                                # create a fake vuid
                                newVUID = str(uuid.uuid4())
                            else:
                                # get list of tokens    
                                tmp_token_list = file.destinationDBlockToken.split(',')
                                # get locations
                                usingT1asT2 = False
                                if job.prodSourceLabel == 'user' and not self.siteMapper.siteSpecList.has_key(computing_site):
                                    dq2IDList = [self.siteMapper.getSite(job.computingSite).ddm_output]
                                else:
                                    if self.siteMapper.getSite(computing_site).cloud != job.getCloud() and \
                                            re.search('_sub\d+$', ds_name) != None and \
                                            (not job.prodSourceLabel in ['user', 'panda']) and \
                                            (not self.siteMapper.getSite(computing_site).ddm_output.endswith('PRODDISK')):
                                        # T1 used as T2. Use both DATADISK and PRODDISK as locations while T1 PRODDISK is phasing out
                                        dq2IDList = [self.siteMapper.getSite(computing_site).ddm_output]
                                        if self.siteMapper.getSite(computing_site).setokens_output.has_key('ATLASPRODDISK'):
                                            dq2IDList += [self.siteMapper.getSite(computing_site).setokens_output['ATLASPRODDISK']]
                                        usingT1asT2 = True
                                    else:
                                        dq2IDList = [self.siteMapper.getSite(computing_site).ddm_output]

                                # use another location when token is set
                                if re.search('_sub\d+$',ds_name) == None and DataServiceUtils.getDestinationSE(file.destinationDBlockToken) != None:
                                    # destination is specified
                                    dq2IDList = [DataServiceUtils.getDestinationSE(file.destinationDBlockToken)]
                                elif (not usingT1asT2) and (not file.destinationDBlockToken in ['NULL', '']):
                                    dq2IDList = []
                                    for tmpToken in tmp_token_list:
                                        # set default
                                        dq2ID = self.siteMapper.getSite(computing_site).ddm_output
                                        # convert token to DQ2ID
                                        if self.siteMapper.getSite(computing_site).setokens_output.has_key(tmpToken):
                                            dq2ID = self.siteMapper.getSite(computing_site).setokens_output[tmpToken]
                                        # replace or append    
                                        if len(tmp_token_list) <= 1 or ds_name != original_name:
                                            # use location consistent with token
                                            dq2IDList = [dq2ID]
                                            break
                                        else:
                                            # use multiple locations for _tid
                                            if not dq2ID in dq2IDList:
                                                dq2IDList.append(dq2ID)

                                # set hidden flag for _sub
                                tmpLifeTime = None
                                tmpMetadata = None
                                if ds_name != original_name and re.search('_sub\d+$', ds_name) != None:
                                    tmpLifeTime = 14
                                    tmpMetadata = {'hidden':True,
                                                   'purge_replicas': 0}

                                # register dataset
                                self.logger.debug('registerNewDataset {ds_name} metadata={meta}'.format(ds_name=ds_name,
                                                                                                     meta=tmpMetadata))
                                status, out = self.__call_retry(rucioAPI.registerDataset, ds_name,metadata=tmpMetadata,
                                                                       lifetime=tmpLifeTime)
                                if not status:
                                    tmp_msg = 'Setupper._setupDestination() could not register : {0}'.format(ds_name)
                                    dest_error[dest] = tmp_msg
                                    self.logger.error(tmp_msg)
                                    continue

                                # register dataset locations
                                if (job.lockedby == 'jedi' and job.prodSourceLabel in ['panda','user']) or \
                                        DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) != None:
                                    # skip registerDatasetLocations
                                    status, out = True, ''
                                elif ds_name == original_name or tmpSrcDDM != tmpDstDDM or \
                                       job.prodSourceLabel == 'panda' or (job.prodSourceLabel in ['ptest','rc_test'] and \
                                                                          job.processingType in ['pathena','prun','gangarobot-rctest']) \
                                       or len(tmp_token_list) > 1 or EventServiceUtils.isMergeAtOS(job.specialHandling):
                                    # set replica lifetime to _sub
                                    repLifeTime = None
                                    if (ds_name != original_name and re.search('_sub\d+$',ds_name) != None) or \
                                            (ds_name == original_name and ds_name.startswith('panda.')):
                                        repLifeTime = 14
                                    elif ds_name.startswith('hc_test') or \
                                            ds_name.startswith('panda.install.') or \
                                            ds_name.startswith('user.gangarbt.'):
                                        repLifeTime = 7
                                    # distributed datasets for ES outputs
                                    grouping = None
                                    if ds_name != original_name and re.search('_sub\d+$', ds_name) is not None \
                                            and EventServiceUtils.isEventServiceJob(job):
                                        dq2IDList = ['type=DATADISK']
                                        grouping = 'NONE'

                                    # register location
                                    isOK = True
                                    for dq2ID in dq2IDList:
                                        activity = DataServiceUtils.getActivityForOut(job.prodSourceLabel)
                                        tmpStr = 'registerDatasetLocation {ds_name} {dq2ID} lifetime={repLifeTime} activity={activity} grouping={grouping}'
                                        self.logger.debug(tmpStr.format(ds_name=ds_name,
                                                                        dq2ID=dq2ID,
                                                                        repLifeTime=repLifeTime,
                                                                        activity=activity,
                                                                        grouping=grouping))
                                        status = False

                                        # invalid location
                                        if dq2ID is None:
                                            out = "wrong location : {0}".format(dq2ID)
                                            self.logger.error(out)
                                            break

                                        status, out = self.__call_retry(rucioAPI.registerDatasetLocation, ds_name,
                                                                        [dq2ID], lifetime=repLifeTime,
                                                                        activity=activity, grouping=grouping)
                                        if not status:
                                            break
                                else:
                                    # skip registerDatasetLocations
                                    status, out = True, ''
                                if not status:
                                    dest_error[dest] = "Could not register location : %s %s" % (ds_name,out.split('\n')[-1])

                        # already failed
                        if dest_error[dest] != '' and ds_name == original_name:
                            break
                        # get vuid
                        if newVUID is None:
                            self.logger.debug('listDatasets '+ds_name)
                            for iDDMTry in range(3):
                                newOut,errMsg = rucioAPI.listDatasets(ds_name)
                                if newOut is None:
                                    time.sleep(10)
                                else:
                                    break
                            if newOut is None:
                                self.logger.error(errMsg)
                            else:
                                self.logger.debug(newOut)
                                newVUID = newOut[ds_name]['vuids'][0]
                        try:
                            # dataset spec
                            ds = DatasetSpec()
                            ds.vuid = newVUID
                            ds.name = ds_name
                            ds.type = 'output'
                            ds.numberfiles = 0
                            ds.currentfiles = 0
                            ds.status = 'defined'
                            # append
                            dataset_map[(ds_name,file.destinationSE, computing_site)] = ds
                        except:
                            # set status
                            errtype,errvalue = sys.exc_info()[:2]
                            self.logger.error("_setupDestination() : %s %s" % (errtype,errvalue))
                            dest_error[dest] = "Setupper._setupDestination() could not get VUID : %s" % ds_name

                # set new destDBlock
                if new_name_map.has_key(dest):
                    file.destinationDBlock = new_name_map[dest]
                # update job status if failed
                if dest_error[dest] != '':
                    job.jobStatus = 'failed'
                    job.ddmErrorCode = ErrorCode.EC_Setupper                
                    job.ddmErrorDiag = dest_error[dest]
                else:
                    newdest = (file.destinationDBlock,file.destinationSE,job.computingSite)
                    # increment number of files
                    dataset_map[newdest].numberfiles = dataset_map[newdest].numberfiles + 1
        # dump
        for tmpDsKey in dataset_map.keys():
            if re.search('_sub\d+$',tmpDsKey[0]) != None:
                self.logger.debug('made sub:%s for nFiles=%s' % (tmpDsKey[0],dataset_map[tmpDsKey].numberfiles))
        # insert datasets to DB
        return self.taskBuffer.insertDatasets(dataset_map.values())

    def _prepare_subscription_metadata(self, job):
        """
        Prepare relevant metadata that needs to be specified in the subscription (e.g. activity, task)
        """

        opt_owner = None
        opt_activity = None
        opt_comment = None

        # set activity
        if job.prodSourceLabel in ['user', 'panda']:
            opt_activity = "Analysis Input"
        elif job.processingType == 'urgent' or job.currentPriority > 1000:
            opt_activity = 'Express'
        else:
            opt_activity = "Production Input"

        # set task ID in comment
        if job.jediTaskID not in ['NULL', 0]:
            opt_comment = 'task_id:{0}'.format(job.jediTaskID)

        return opt_owner, opt_activity, opt_comment

    def _subscribeDistpatchDB(self):
        """
        Subscribe distpatchDBlocks to sites
        :return:
        """

        dispatch_ds_errors  = {} # dispatch block to error map
        failed_jobs = [] # list of failed jobs

        for job in self.jobs:
            # ignore failed jobs
            if job.jobStatus in ['failed','cancelled'] or job.isCancelled():
                continue
            # ignore no dispatch jobs
            if job.dispatchDBlock=='NULL' or job.computingSite=='NULL':
                continue

            # extract dispatchDBlock and computingSite
            disp = (job.dispatchDBlock, job.computingSite)
            if dispatch_ds_errors.has_key(disp) == 0:
                dispatch_ds_errors[disp] = ''

                # TODO: here you need the better logic of a distributed dataset
                # TODO: is there any precaution to take in case of staging?
                # destination endpoints
                computing_site_spec = self.siteMapper.getSite(job.computingSite)

                # 1. Find the location of the input files on associated storages
                status, dst_endpoints = self.__call_retry(rucioAPI.list_file_replicas, job.dispatchDBlock, computing_site_spec)
                if not status:
                    continue


                # 2. Unless Rucio is not in use, prepare the subscription
                if (not self.pandaDDM) and job.prodSourceLabel != 'ddm':

                    opt_owner, opt_activity, opt_comment = self._prepare_subscription_metadata(self, job)

                    # TODO: this part needs the Rucio distributed dataset logic
                    # http://rucio.readthedocs.io/en/latest/api/rule.html
                    # register subscription
                    # The key parameter to change is the grouping. Per default this is DATASET, but if you set this
                    # to NONE then the files of the dataset are distributed (randomly), assuming that the
                    # rse_expression covers more than 1 RSE. There is not awful lot documentation about the exact
                    # mechanic of how the file distribution works, but it is anyway quite simple:
                    # The algorithm prioritizes re-using existing replicas over anything else. Otherwise it chooses
                    # the destinations randomly, unless the weight parameter is chosen when doing add_replication_rule.
                    # Then it distributes according to the weight (e.g. mou, freespace, etc.)

                    self.logger.debug('{0} {1} {2}'.format('registerDatasetSubscription',
                                                           (job.dispatchDBlock, dst_endpoints),
                                                           {'activity': opt_activity, 'lifetime': 7,
                                                            'dn': opt_owner, 'comment': opt_comment, 'grouping': 'NONE'}))

                    status, out = self.__call_retry(rucioAPI.registerDatasetSubscription, job.dispatchDBlock,
                                                    dst_endpoints, activity=opt_activity, lifetime=7, dn=opt_owner,
                                                    comment=opt_comment, grouping='NONE')
                    if not status or not out:
                        self.logger.error(out)
                        dispatch_ds_errors[disp] = "Setupper._subscribeDistpatchDB() could not register subscription"
                    else:
                        self.logger.debug(out)

            # failed jobs
            if dispatch_ds_errors[disp] != '':
                job.jobStatus = 'failed'
                job.ddmErrorCode = ErrorCode.EC_Setupper                
                job.ddmErrorDiag = dispatch_ds_errors[disp]
                failed_jobs.append(job)

        # update failed jobs only, succeeded jobs should be activated by DDM callback
        self.updateFailedJobs(failed_jobs)

    def __clean_lfn(self, dirty_lfn):
        # remove the attempt number at the end of the LFN
        return re.sub('\.\d+$', '', dirty_lfn)

    def __collect_input_lfns(self):
        # iterate the jobs and its files to make a list with all the LFNs (with and without attempt number)
        input_lfn_list = set()

        # iterate jobs
        for tmpJob in self.jobs:
            for file_tmp in tmpJob.Files:
                if file_tmp.type == 'input':

                    # add the LFN with and without attempt number to the list
                    input_lfn_list.add(file_tmp.lfn)
                    gen_lfn = self.__clean_lfn(file_tmp.lfn)
                    input_lfn_list.add(gen_lfn)

                    if file_tmp.GUID not in ['NULL', '', None]:
                        if not file_tmp.dataset in self.lfnDatasetMap:
                            self.lfnDatasetMap[file_tmp.dataset] = {}
                        self.lfnDatasetMap[file_tmp.dataset][file_tmp.lfn] = {'guid': file_tmp.GUID,
                                                                              'chksum': file_tmp.checksum,
                                                                              'md5sum': file_tmp.md5sum,
                                                                              'fsize': file_tmp.fsize,
                                                                              'scope': file_tmp.scope}
        return input_lfn_list

    def __get_file_metadata(self, dataset, input_lfn_list, dataset_lfn_map, lfn_dataset_map, missing_datasets, input_ds_errors):
        """
        Does a rucio.list_files of the dataset to retrieve the file metadata (checksum, size, events...)
        """

        file_metadata = {}

        status, out = self.getListFilesInDataset(dataset, input_lfn_list)
        if status != 0:
            self.logger.error(out)
            input_ds_errors[dataset] = 'could not get file list of prodDBlock %s' % dataset
            self.logger.error(input_ds_errors[dataset])
            if status == -1:
                missing_datasets[dataset] = 'DS:{0} not found in DDM'.format(dataset)
            else:
                missing_datasets[dataset] = out
        else:
            # make map (key: LFN w/o attemptNr, value: LFN with attemptNr)
            file_list = out
            try:
                # loop over all files
                for tmp_lfn, file_attribs in file_list.iteritems():
                    file_metadata[tmp_lfn] = file_metadata
                    gen_lfn = self.__clean_lfn(tmp_lfn)
                    if dataset_lfn_map[dataset].has_key(gen_lfn):
                        # get attemptNr
                        new_attempt_nr = 0
                        new_mat = re.search('\.(\d+)$', tmp_lfn)
                        if new_mat != None:
                            new_attempt_nr = int(new_mat.group(1))
                        old_attempt_nr = 0
                        old_mat = re.search('\.(\d+)$', dataset_lfn_map[dataset][gen_lfn])
                        if old_mat is not None:
                            old_attempt_nr = int(old_mat.group(1))
                        # keep the highest attempt number in the lfn map
                        if new_attempt_nr > old_attempt_nr:
                            dataset_lfn_map[dataset][gen_lfn] = tmp_lfn
                    else:
                        dataset_lfn_map[dataset][gen_lfn] = tmp_lfn

                    lfn_dataset_map[dataset_lfn_map[dataset][gen_lfn]] = dataset
            except:
                input_ds_errors[dataset] = 'could not convert HTTP-res to map for prodDBlock %s' % dataset
                self.logger.error(input_ds_errors[dataset])
                self.logger.error(out)

        return file_metadata

    def __get_dataset_replica_locations(self, job, dataset, input_ds_errors, replica_map):
        # get dataset replica locations

        if job.prodSourceLabel in ['managed', 'test'] and input_ds_errors[dataset] == '' \
                and (not replica_map.has_key(dataset)):
            if dataset.endswith('/'):
                status, out = self.getListDatasetReplicasInContainer(dataset)
            else:
                status, out = self.getListDatasetReplicas(dataset, False)

            if status != 0 or out.startswith('Error'):
                input_ds_errors[dataset] = 'could not get locations for {0}'.format(dataset)
                self.logger.error(input_ds_errors[dataset])
                self.logger.error(out)
            else:
                tmp_replicas_sites = {}
                try:
                    # convert res to map
                    exec "tmp_replicas_sites = %s" % out
                    replica_map[dataset] = tmp_replicas_sites
                except:
                    input_ds_errors[dataset] = 'could not convert HTTP-res to replica map {0}'.format(dataset)
                    self.logger.error(input_ds_errors[dataset])
                    self.logger.error(out)

                # append except DBR
                if not dataset.startswith('ddo'):
                    self.replica_mapForBroker[dataset] = tmp_replicas_sites

        return

    def __is_failed_job(self, job, datasets, missing_datasets, jobs_failed, jobs_waiting, input_ds_errors):
        """
        Checks whether the dataset is missing or there was another error
        """

        # check the job for missing datasets. Set the job as failed and the files as missing
        for dataset in datasets:
            if dataset in missing_datasets:
                job.jobStatus = 'failed'
                job.ddmErrorCode = ErrorCode.EC_GUID
                job.ddmErrorDiag = missing_datasets[dataset]

                for file_tmp in job.Files:
                    if file_tmp.dataset == dataset:
                        file_tmp.status = 'missing'

                # append job to failed list
                jobs_failed.append(job)
                self.logger.debug("{0} failed with {1}".format(job.PandaID, missing_datasets[dataset]))
                return True

        # check for waiting
        for dataset in datasets:
            if input_ds_errors[dataset] != '':
                # append job to waiting list
                jobs_waiting.append(job)
                return True

        return False

    def __update_file_info(self, job, jobs_processed, dataset_lfn_map, file_metadata, jobs_failed, all_lfns, all_guids, all_scopes):
        """
        Iterates the files updating their metadata and replacing the LFN
        """
        replace_list = []

        all_lfns = {}
        all_guids = {}
        all_scopes = {}

        is_failed = False
        for file in job.Files:
            if not (file.type == 'input' and file.dispatchDBlock == 'NULL'):
                continue

            add_to_lfn_map = True
            if file.GUID == 'NULL':
                clean_lfn = self.__clean_lfn(file.lfn)
                # get the lfn with the last attempt number
                if clean_lfn == file.lfn and clean_lfn in dataset_lfn_map[file.dataset].keys():
                    file.lfn = dataset_lfn_map[file.dataset][clean_lfn]
                    replace_list.append((clean_lfn, file.lfn))

                # set file metadata
                if file.lfn in file_metadata:
                    file.GUID = file_metadata[file.lfn]['guid']
                    file.fsize = file_metadata[file.lfn]['fsize']
                    file.md5sum = file_metadata[file.lfn]['md5sum']
                    file.checksum = file_metadata[file.lfn]['chksum']
                    file.scope = file_metadata[file.lfn]['scope']
                    # remove white space
                    if file.md5sum is not None:
                        file.md5sum = file.md5sum.strip()
                    if file.checksum is not None:
                        file.checksum = file.checksum.strip()
            else:
                if not job.prodSourceLabel in ['managed', 'test']:
                    add_to_lfn_map = False

            # check for missing file
            if file.GUID == 'NULL' or job.prodSourceLabel in ['managed', 'test']:
                if file.lfn not in file_metadata:
                    # append job to waiting list
                    err_msg = "GUID for {0} not found in Rucio".format(file.lfn)
                    self.logger.error(err_msg)
                    file.status = 'missing'
                    if job not in jobs_failed:
                        job.jobStatus = 'failed'
                        job.ddmErrorCode = ErrorCode.EC_GUID
                        job.ddmErrorDiag = err_msg

                        jobs_failed.append(job)
                        is_failed = True
                    continue

            # add to allLFNs/allGUIDs
            if add_to_lfn_map:
                cloud = job.getCloud()

                all_lfns.setdefault(cloud, [])
                all_guids.setdefault(cloud, [])
                all_scopes.setdefault(cloud, [])

                all_lfns[cloud].append(file.lfn)
                all_guids[cloud].append(file.GUID)
                all_scopes[cloud].append(file.scope)

        # update the LFN with last attempt number in jobParameters
        if not is_failed:
            for patt, repl in replace_list:
                job.jobParameters = re.sub('%s ' % patt, '%s ' % repl, job.jobParameters)
            # append job to processed list
            jobs_processed.append(job)

        return all_lfns, all_guids, all_scopes

    def __create_dataset_replica_map(self, input_lfn_list):

        replica_map = {}
        dataset_lfn_map = {} # dictionary mapping datasets to LFNs
        lfn_dataset_map = {} # dictionary mapping LFNs to datasets

        input_ds_errors = {}
        missing_datasets = {} # dictionary mapping datasets to reason for missing

        jobs_processed, jobs_failed, jobs_waiting = [], [], []
        all_lfns, all_guids, all_scopes = {}, {}, {}

        # loop over all jobs to collect datasets, resolve the files and create a dataset replica map
        for job in self.jobs:

            # set job failed if the computingSite does not exist
            if job.computingSite != 'NULL' and (not job.computingSite in self.siteMapper.siteSpecList.keys()):
                job.jobStatus = 'failed'
                job.ddmErrorCode = ErrorCode.EC_Setupper
                job.ddmErrorDiag = 'computingSite:{0} is unknown'.format(job.computingSite)
                jobs_processed.append(job)
                continue

            # ignore no prodDBlock jobs or container dataset
            if job.prodDBlock == 'NULL':
                # append job to processed list
                jobs_processed.append(job)
                continue

            # make a list of all input datasets
            datasets = []
            for file in job.Files:
                if file.type == 'input' and file.dispatchDBlock == 'NULL' \
                        and (file.GUID == 'NULL' or job.prodSourceLabel in ['managed', 'test', 'ptest']):
                    if file.dataset not in datasets:
                        datasets.append(file.dataset)

            # get LFN list
            for dataset in datasets:
                if not dataset in dataset_lfn_map.keys():
                    input_ds_errors[dataset] = ''
                    dataset_lfn_map[dataset] = {}

                    # get the file metadata (checksum, size, events...)
                    file_metadata = self.__get_file_metadata(dataset, input_lfn_list, dataset_lfn_map, input_ds_errors,
                                                             missing_datasets, lfn_dataset_map)

                    # get replica locations
                    self.__get_dataset_replica_locations(job, dataset, input_ds_errors, replica_map)

            # check for failed jobs
            if self.__is_failed_job(job, datasets, missing_datasets, jobs_failed, jobs_waiting, input_ds_errors):
                continue

            # update the file metadata and LFN
            all_lfns, all_guids, all_scopes = self.__update_file_info(self, job, jobs_processed, dataset_lfn_map, file_metadata, jobs_failed)

        return dataset_lfn_map, lfn_dataset_map, replica_map, missing_datasets, \
               jobs_processed, jobs_failed, jobs_waiting,\
               all_lfns, all_guids, all_scopes

    def __retrieve_file_replicas(self, all_lfns, all_guids, all_scopes, replica_map, lfn_dataset_map):

        for cloud, cloud_lfns in all_lfns.iteritems():

            # skip user and HC jobs
            if len(self.jobs) > 0 and (self.jobs[0].prodSourceLabel in ['user', 'panda', 'ddm'] or \
                                       self.jobs[0].processingType.startswith('gangarobot') or \
                                       self.jobs[0].processingType.startswith('hammercloud')):
                continue

            # initialize cloud if needed
            self.file_replicas.setdefault(cloud, {})

            # loop over all files to retrieve the datasets
            for tmp_lfn in cloud_lfns:

                # skip if dataset is missing
                if tmp_lfn not in lfn_dataset_map:
                    continue

                tmp_dataset = lfn_dataset_map[tmp_lfn]
                if tmp_dataset not in self.file_replicas[cloud]:
                    # collect sites
                    tmp_site_ddm_map = DataServiceUtils.getSitesWithDataset(tmp_dataset, self.siteMapper, replica_map,
                                                                            cloud, getDQ2ID=True)
                    if tmp_site_ddm_map == {}:
                        continue

                    # initialize the dataset's file replica map
                    self.file_replicas[cloud][tmp_dataset] = {'allfiles':[], 'allguids':[], 'allscopes':[], 'sites':{}}

                    for tmp_site in tmp_site_ddm_map.keys():
                        self.file_replicas[cloud][tmp_dataset]['sites'][tmp_site] = []

                    self.file_replicas[cloud][tmp_dataset]['siteDQ2IDs'] = tmp_site_ddm_map

                # add files
                if tmp_lfn not in self.file_replicas[cloud][tmp_dataset]:
                    self.file_replicas[cloud][tmp_dataset]['allfiles'].append(tmp_lfn)
                    index_tmp_lfn = all_lfns[cloud].index(tmp_lfn)
                    self.file_replicas[cloud][tmp_dataset]['allguids'].append(all_guids[cloud][index_tmp_lfn])
                    self.file_replicas[cloud][tmp_dataset]['allscopes'].append(all_scopes[cloud][index_tmp_lfn])

            # get available files at each site
            for tmp_dataset in self.file_replicas[cloud].keys():

                check_se_map = {}

                # TODO: check with Tadashi, I don't understand why he seems to be looking at tapes only?
                for tmp_site in self.file_replicas[cloud][tmp_dataset]['sites'].keys():
                    tmp_site_spec = self.siteMapper.getSite(tmp_site)
                    if tmp_site not in check_se_map:
                        check_se_map[tmp_site] = []
                        check_se_map[tmp_site] += tmp_site_spec.ddm_endpoints_input.getTapeEndPoints()

                # for tmpCatURL in checkLfcSeMap.keys():

                # get SEs
                tmp_se_list = []
                tmp_site_name_list = []
                for tmp_site in check_se_map.keys():
                    tmp_se_list += check_se_map[tmp_site]
                    tmp_site_name_list.append(tmp_site)

                # get available file list
                self.logger.debug('checking {0} {1}'.format(tmp_se_list, tmp_site_name_list))
                tmp_stat, bulk_av_files = rucioAPI.listFileReplicas(self.file_replicas[cloud][tmp_dataset]['allscopes'],
                                                                    self.file_replicas[cloud][tmp_dataset]['allfiles'],
                                                                    tmp_se_list)
                if not tmp_stat:
                    self.logger.error('failed to get file replicas')
                    bulk_av_files = {}

                # iterate the SEs to check and
                for tmp_site, tmp_site_ses in check_se_map.iteritems():
                    tmp_site_ses = set(tmp_site_ses)
                    self.file_replicas[cloud][tmp_dataset]['sites'][tmp_site] = []
                    for tmp_lfn_ck, tmp_file_ses in bulk_av_files.iteritems():
                        site_has_file = False
                        for tmp_file_se in tmp_file_ses:
                            if tmp_file_se in tmp_site_ses:
                                site_has_file = True
                                break

                        # append
                        if site_has_file:
                            self.file_replicas[cloud][tmp_dataset]['sites'][tmp_site].append(tmp_lfn_ck)

                    n_files = len(self.file_replicas[cloud][tmp_dataset]['sites'][tmp_site])
                    self.logger.debug('{0} files available at cloud={1} site={2} for dataset={3}'.format(n_files, cloud,
                                                                                                         tmp_site,
                                                                                                         tmp_dataset))

    def _input_dataset_file_preparation(self):

        self.logger.debug('Input datasets and files preparation')

        input_lfn_list = self.__collect_input_lfns()

        ret_variables = self.__create_dataset_replica_map(input_lfn_list)
        # unpack the results
        dataset_lfn_map, lfn_dataset_map, replica_map, missing_datasets, jobs_processed, jobs_failed, jobs_waiting, \
        all_lfns, all_guids, all_scopes= ret_variables

        # resolves file replicas
        self.__retrieve_file_replicas(all_lfns, all_guids, all_scopes, replica_map, lfn_dataset_map)
        # TODO: there is some part to fill the missing files at the computingSite that needs to be written

        # splits the dataset name to retrieve summary fields such as project and filetype
        for tmp_job in self.jobs:
            try:
                # set only for production/analysis/test
                if not tmp_job.prodSourceLabel in ['managed', 'test', 'rc_test', 'ptest', 'user', 'prod_test']:
                    continue

                # loop over all files
                tmp_job.nInputDataFiles = 0
                tmp_job.inputFileBytes = 0
                tmp_input_file_project = None
                tmp_input_file_type = None
                for file_tmp in tmp_job.Files:
                    # use input files and ignore DBR/lib.tgz
                    if file_tmp.type == 'input' and not file_tmp.dataset.startswith('ddo') \
                            and not file_tmp.lfn.endswith('.lib.tgz'):
                        tmp_job.nInputDataFiles += 1
                        if file_tmp.fsize not in ['NULL',None,0,'0']:
                            tmp_job.inputFileBytes += file_tmp.fsize

                        # get input type and project
                        if tmp_input_file_project is None:
                            tmp_input_items = file_tmp.dataset.split('.')
                            # input project
                            tmp_input_file_project = tmp_input_items[0].split(':')[-1]
                            # input type. ignore user/group/groupXY 
                            if len(tmp_input_items) > 4 and (not tmp_input_items[0] in ['', 'NULL', 'user', 'group']) \
                                    and not tmp_input_items[0].startswith('group') \
                                    and not file_tmp.dataset.startswith('panda.um.'):
                                tmp_input_file_type = tmp_input_items[4]

                # set input type and project
                if not tmp_job.prodDBlock in ['', None, 'NULL']:
                    # input project
                    if tmp_input_file_project is not None:
                        tmp_job.inputFileProject = tmp_input_file_project
                    # input type
                    if tmp_input_file_type is not None:
                        tmp_job.inputFileType = tmp_input_file_type

                # protection
                max_input_file_bytes = 99999999999
                if tmp_job.inputFileBytes > max_input_file_bytes:
                    tmp_job.inputFileBytes = max_input_file_bytes

                # set background-able flag
                tmp_job.setBackgroundableFlag()

            except:
                err_type, err_value = sys.exc_info()[:2]
                self.logger.error("failed to set data summary fields for PandaID={0}: {1} {2}".
                                  format(tmp_job.PandaID, err_type, err_value))

        # send waiting jobs to jobsWaiting4 table
        self.taskBuffer.keepJobs(jobs_waiting)
        # update failed job
        self.updateFailedJobs(jobs_failed)
        # remove waiting/failed jobs
        self.jobs = jobs_processed

        # delete huge variables
        del all_lfns
        del all_guids
        del all_scopes

        del dataset_lfn_map
        del lfn_dataset_map
        del replica_map
        del missing_datasets

        del jobs_processed
        del jobs_failed
        del jobs_waiting

    def removeWaitingJobs(self):
        """
        Iterate self.jobs and put waiting jobs into a separate list
        """
        jobs_waiting = []
        jobs_processed = []

        # iterate jobs and separate waiting jobs
        for tmp_job in self.jobs:
            if tmp_job.jobStatus == 'waiting':
                jobs_waiting.append(tmp_job)
            else:
                jobs_processed.append(tmp_job)

        # move waiting jobs to jobsWaiting4 table
        self.taskBuffer.keepJobs(jobs_waiting)

        # remove waiting/failed jobs
        self.jobs = jobs_processed

    # memory checker
    def _memoryCheck(self):
        try:
            import os
            proc_status = '/proc/%d/status' % os.getpid()
            procfile = open(proc_status)
            name   = ""
            vmSize = ""
            vmRSS  = ""
            # extract Name,VmSize,VmRSS
            for line in procfile:
                if line.startswith("Name:"):
                    name = line.split()[-1]
                    continue
                if line.startswith("VmSize:"):
                    vmSize = ""
                    for item in line.split()[1:]:
                        vmSize += item
                    continue
                if line.startswith("VmRSS:"):
                    vmRSS = ""
                    for item in line.split()[1:]:
                        vmRSS += item
                    continue
            procfile.close()
            self.logger.debug('MemCheck PID=%s Name=%s VSZ=%s RSS=%s' % (os.getpid(),name,vmSize,vmRSS))
        except:
            errtype,errvalue = sys.exc_info()[:2]
            self.logger.error("memoryCheck() : %s %s" % (errtype,errvalue))
            self.logger.debug('MemCheck PID=%s unknown' % os.getpid())
            return

    # get list of files in dataset
    def getListFilesInDataset(self, dataset, file_list=None, useCache=True):

        # return cache data if available
        if useCache and self.lfnDatasetMap.has_key(dataset):
            return 0, self.lfnDatasetMap[dataset]

        # query Rucio directly
        for iDDMTry in range(3):
            try:
                self.logger.debug('listFilesInDataset {0}'.format(dataset))
                items, tmpDummy = rucioAPI.listFilesInDataset(dataset, file_list=file_list)
                status = 0
                break
            except DataIdentifierNotFound:
                status = -1
                break
            except:
                status = -2

        # there has been a problem
        if status != 0:
            errType, errValue = sys.exc_info()[:2]
            out = '{0} {1}'.format(errType, errValue)
            return status, out

        # keep to avoid redundant lookup
        self.lfnDatasetMap[dataset] = items
        return status, items

    def getListDatasetInContainer(self, container):
        """
        get list of datasets in container
        :param container:
        :return:
        """
        self.logger.debug('listDatasetsInContainer '+container)
        for iDDMTry in range(3):
            datasets, out = rucioAPI.listDatasetsInContainer(container)
            if datasets is None:
                time.sleep(10)
            else:
                break
        # there has been a problem
        if datasets is None:
            self.logger.error(out)
            return False, out

        return True, datasets

    def getListDatasetReplicasInContainer(self, container, getMap=False):

        # get datasets in container
        self.logger.debug('listDatasetsInContainer '+container)
        for iDDMTry in range(3):
            datasets, out = rucioAPI.listDatasetsInContainer(container)
            if datasets is None:
                time.sleep(10)
            else:
                break
        if datasets is None:
            self.logger.error(out)
            return 1, out

        # generate a map with the number of files on each RSE
        replica_map = {}

        for dataset in datasets:
            self.logger.debug('listDatasetReplicas {0}'.format(dataset))
            status, out = self.getListDatasetReplicas(dataset, False)
            self.logger.debug(out)
            if status != 0 or out.startswith('Error'):
                return status, out

            # convert map
            tmp_replicas_sites = {}
            try:
                exec "tmp_replicas_sites = %s" % out
            except:
                return status,out

            # get map is requested
            if getMap:
                replica_map[dataset] = tmp_replicas_sites
                continue

            # otherwise aggregate
            for rse, file_stats in tmp_replicas_sites.iteritems():
                if rse not in replica_map:
                    replica_map[rse] = [file_stats[-1]]
                else:
                    new_stats_map = {}
                    for st_name, st_value in allRepMap[rse][0].iteritems():
                        if file_stats[-1].has_key(st_name):
                            # try mainly for archived=None
                            try:
                                new_stats_map[st_name] = st_value + file_stats[-1][st_name]
                            except:
                                new_stats_map[st_name] = st_value
                        else:
                            new_stats_map[st_name] = st_value

                    replica_map[rse] = [new_stats_map]

        # return
        self.logger.debug('listDatasetsInContainer resolved {0}'.format(replica_map))
        if not getMap:
            return 0, str(replica_map)
        else:
            return 0, replica_map

    def getListDatasetReplicas(self, dataset, getMap=True):
        """
        Get list of replicas for a dataset
        :param dataset:
        :param getMap:
        :return:
        """
        nTry = 3
        for iDDMTry in range(nTry):
            self.logger.debug("%s/%s listDatasetReplicas %s" % (iDDMTry,nTry,dataset))
            status, out = rucioAPI.listDatasetReplicas(dataset)
            if status != 0:
                time.sleep(10)
            else:
                break

        # There has been a problem in the Rucio call
        if status != 0:
            self.logger.error(out)
            self.logger.error('bad response for {0}'.format(dataset))
            if getMap:
                return False, {}
            else:
                return 1, str({})

        try:
            retMap = out
            self.logger.debug('getListDatasetReplicas->{0}'.format(retMap))
            if getMap:
                return True, retMap
            else:
                return 0, str(retMap)
        except:
            self.logger.error(out)            
            self.logger.error('could not convert HTTP-res to replica map for %s' % dataset)
            if getMap:
                return False, {}
            else:
                return 1, str({})

    def _dynamicDataPlacement(self):
        """
        Dynamic data placement for analysis jobs
        :return:
        """
        # only first submission
        if not self.firstSubmission:
            return

        # no jobs
        if len(self.jobs) == 0:
            return

        # only successful analysis
        if self.jobs[0].jobStatus in ['failed','cancelled'] or self.jobs[0].isCancelled() \
                or (not self.jobs[0].prodSourceLabel in ['user','panda']):
            return

        # disable for JEDI
        if self.jobs[0].lockedby == 'jedi':
            return

        # execute
        self.logger.debug('execute PD2P')
        from DynDataDistributer import DynDataDistributer
        ddd = DynDataDistributer(self.jobs, self.taskBuffer, self.siteMapper)
        ddd.run()
        self.logger.debug('finished PD2P')
        return

    def _makeDisDatasetsForExistingfiles(self):
        """
        Make dispatch datasets for existing files (for missing file the dispatch dataset was done beforehand) to avoid deletion while jobs are queued.
        :return:
        """

        self.logger.debug('make dis datasets for existing files')
        # collect existing files
        dataset_file_map = {}
        max_jobs  = 20
        njobs_map  = {}

        # generate the dataset file map
        for job_tmp in self.jobs:

            # use production or test jobs only
            if job_tmp.prodSourceLabel not in ['managed', 'test']:
                continue
            # skip for prefetcher or transferType=direct
            if job_tmp.usePrefetcher() or job_tmp.transferType == 'direct':
                continue
            # ignore inappropriate status
            if job_tmp.jobStatus in ['failed', 'cancelled', 'waiting'] or job_tmp.isCancelled():
                continue

            # skip ND cloud
            if job_tmp.getCloud() == 'ND' and self.siteMapper.getSite(job_tmp.computingSite).cloud == 'ND':
                continue

            # check SE to use T2 only
            # TODO: can this block be removed? I don't understand the purpose
            tmpSrcID = self.siteMapper.getCloud(job_tmp.getCloud())['source']
            srcSiteSpec = self.siteMapper.getSite(tmpSrcID)
            dstSiteSpec = self.siteMapper.getSite(job_tmp.computingSite)
            if dstSiteSpec.ddm_endpoints_input.isAssociated(srcSiteSpec.ddm_input):
                continue

            # look for log _sub dataset to be used as a key
            log_dataset_name = ''
            for file_tmp in job_tmp.Files:
                if file_tmp.type == 'log':
                    log_dataset_name = file_tmp.destinationDBlock
                    break

            # TODO: you should not blindly register the files to the default input
            dest_rse = self.siteMapper.getSite(job_tmp.computingSite).ddm_input

            map_key_job = (dest_rse, log_dataset_name)
            njobs_map.setdefault(map_key_job, 0)
            map_key = (dest_rse, log_dataset_name, njobs_map[map_key_job] / max_jobs)

            # increment the number of jobs per key
            njobs_map[map_key_job] += 1

            dataset_file_map.setdefault(map_key, {})

            # add files
            for file_tmp in job_tmp.Files:

                # skip output and log files
                if file_tmp.type != 'input':
                    continue

                # if files are unavailable at the dest site, it means normal dis datasets contain them or files are cached
                if not file_tmp.status in ['ready']:
                    continue

                # TODO: understand how this works
                real_dest_rse = (dest_rse, )

                default_job_entry = {'taskID': job_tmp.taskID, 'PandaID': job_tmp.PandaID, 'files': {}}
                dataset_file_map[map_key].setdefault(real_dest_rse, default_job_entry)

                default_file_entry = {'lfn': '{0}:{1}'.format(file_tmp.scope, file_tmp.lfn), 'guid': file_tmp.GUID, 'fileSpecs': []}
                dataset_file_map[map_key][real_dest_rse]['files'].setdefault(file_tmp.lfn, default_file_entry)

                # add file spec
                dataset_file_map[map_key][real_dest_rse]['files'][file_tmp.lfn]['fileSpecs'].append(file_tmp)

        dispatch_block_list = []

        # iterate the dataset_file_map to generate, close and subscribe the dispatch blocks
        for map_key_tmp, dummy_value_tmp in dataset_file_map.iteritems():

            dummy_location_tmp, log_dataset_name_tmp, bunch_index_tmp = map_key_tmp

            # iterate the real_dest_rse
            for location_list_tmp, value_tmp in dummy_value_tmp.iteritems():

                # iterate the rse's in real_dest_rse
                for location_tmp in location_list_tmp:
                    file_list_tmp = value_tmp['files']

                    if file_list_tmp == {}:
                        continue

                    max_files = 500
                    i_files = 0
                    i_loop = 0

                    # iterate the files. they should all belong to the same job
                    while i_files < len(file_list_tmp):
                        sub_file_names = file_list_tmp.keys()[i_files:i_files+max_files]
                        if len(sub_file_names) == 0:
                            break

                        # generate the name for the dispatch block
                        dispatch_block_name = "panda.%s.%s.%s.%s_dis0%s%s" % (value_tmp['taskID'],
                                                                              time.strftime('%m.%d'), 'GEN',
                                                                              commands.getoutput('uuidgen'), i_loop,
                                                                              value_tmp['PandaID'])
                        i_files += max_files
                        lfns = []
                        guids = []
                        fsizes = []
                        chksums = []

                        # generate the lists with the files and its metadata
                        for tmp_sub_file_name in sub_file_names:

                            # get the file metadata
                            lfns.append(file_list_tmp[tmp_sub_file_name]['lfn'])
                            guids.append(file_list_tmp[tmp_sub_file_name]['guid'])
                            fsizes.append(long(file_list_tmp[tmp_sub_file_name]['fileSpecs'][0].fsize))
                            chksums.append(file_list_tmp[tmp_sub_file_name]['fileSpecs'][0].checksum)

                            # set the dispatch block name
                            for file_spec_tmp in file_list_tmp[tmp_sub_file_name]['fileSpecs']:
                                if file_spec_tmp.status in ['ready'] and file_spec_tmp.dispatchDBlock == 'NULL':
                                    file_spec_tmp.dispatchDBlock = dispatch_block_name

                        # register datasets
                        metadata = {'hidden':True, 'purge_replicas': 0}
                        if value_tmp['taskID'] not in [None, 'NULL']:
                            metadata['task_id'] = str(value_tmp['taskID'])

                        msg_tmp = 'ext registerNewDataset {ds} {lfns} {guids} {fsizes} {chksums} {meta}'
                        self.logger.debug(msg_tmp.format(ds=dispatch_block_name, lfns=str(lfns), guids=str(guids),
                                                         fsizes=str(fsizes), chksums=str(chksums),
                                                         meta=str(metadata)))

                        status, out = self.__call_retry(rucioAPI.registerDataset, dispatch_block_name, lfns, guids,
                                                        fsizes, chksums, lifetime=7, scope='panda', metadata=metadata)
                        if not status:
                            # do not attempt to subscribe the failed dataset
                            continue

                        # get the VUID from the response
                        try:
                            exec "vuid = {0}['vuid']".format(str(out))
                            # dataset spec. currentfiles is used to count the number of failed jobs
                            dispatch_block = DatasetSpec()
                            dispatch_block.vuid = vuid
                            dispatch_block.name = dispatch_block_name
                            dispatch_block.type = 'dispatch'
                            dispatch_block.status = 'defined'
                            dispatch_block.numberfiles  = len(lfns)
                            dispatch_block.currentfiles = 0
                            dispatch_block_list.append(dispatch_block)
                        except:
                            err_type, err_value = sys.exc_info()[:2]
                            self.logger.error('ext registerNewDataset : failed to decode VUID for {0} - {1} {2}'
                                              .format(dispatch_block_name, err_type, err_value))
                            continue

                        # close dispatch dataset
                        self.logger.debug('freezeDataset {0}'.format(dispatch_block_name))
                        status, out = self.__call_retry(rucioAPI.closeDataset, dispatch_block_name)
                        if not status:
                            continue

                        # register location
                        self.logger.debug('ext registerDatasetLocation {ds} {dq2ID} {lifeTime}days asynchronous=True'.
                                          format(ds=dispatch_block_name, dq2ID=location_tmp, lifeTime=7))

                        status, out = self.__call_retry(rucioAPI.registerDatasetLocation, dispatch_block_name,
                                                        [location_tmp], 7, activity='Production Input', scope='panda',
                                                        asynchronous=True, grouping='NONE')
                        if not status:
                            continue

        # insert datasets to DB
        self.taskBuffer.insertDatasets(dispatch_block_list)
        self.logger.debug('finished to make dis datasets for existing files')
        return

    # setup jumbo jobs
    def _setupJumbojobs(self):
        if len(self.jumboJobs) == 0:
            return
        self.logger.debug('setup jumbo jobs')
        # get files in datasets
        dsLFNsMap = {}
        failedDS  = set()
        for jumboJobSpec in self.jumboJobs:
            for file_tmpSpec in jumboJobSpec.Files:
                # only input
                if not tmpFileSpec.type in ['input']:
                    continue
                # get files
                if not tmpFileSpec.dataset in dsLFNsMap:
                    if not tmpFileSpec.dataset in failedDS:
                        tmpStat,tmpMap = self.getListFilesInDataset(tmpFileSpec.dataset,
                                                                    useCache=False)
                        # failed
                        if tmpStat != 0:
                            failedDS.add(tmpFileSpec.dataset)
                            self.logger.debug('failed to get files in {0} with {1}'.format(tmpFileSpec.dataset,
                                                                                           tmpMap))
                        else:
                            # append
                            dsLFNsMap[tmpFileSpec.dataset] = tmpMap
                # set failed if file lookup failed
                if tmpFileSpec.dataset in failedDS:
                    jumboJobSpec.jobStatus    = 'failed'
                    jumboJobSpec.ddmErrorCode = ErrorCode.EC_GUID
                    jumboJobSpec.ddmErrorDiag = 'failed to get files in {0}'.format(tmpFileSpec.dataset)
                    break
        # make dis datasets
        okJobs = []
        ngJobs = []
        for jumboJobSpec in self.jumboJobs:
            # skip failed
            if jumboJobSpec.jobStatus == 'failed':
                ngJobs.append(jumboJobSpec)
                continue
            # get datatype
            try:
                tmpDataType = jumboJobSpec.prodDBlock.split('.')[-2]
                if len(tmpDataType) > 20:
                    raise RuntimeError,''
            except:
                # default
                tmpDataType = 'GEN'
            # files for jumbo job
            lfnsForJumbo = self.taskBuffer.getLFNsForJumbo(jumboJobSpec.jediTaskID)
            # make dis dataset name 
            dispatchDBlock = "panda.%s.%s.%s.%s_dis%s" % (jumboJobSpec.taskID,time.strftime('%m.%d.%H%M'),tmpDataType,
                                                          'jumbo',jumboJobSpec.PandaID)
            # collect file attributes
            lfns = []
            guids = []
            sizes = []
            checksums = []
            for tmpFileSpec in jumboJobSpec.Files:
                # only input
                if not tmpFileSpec.type in ['input']:
                    continue
                for tmpLFN,tmpVar in dsLFNsMap[tmpFileSpec.dataset].iteritems():
                    tmpLFN = '{0}:{1}'.format(tmpVar['scope'],tmpLFN)
                    if tmpLFN not in lfnsForJumbo:
                        continue
                    lfns.append(tmpLFN)
                    guids.append(tmpVar['guid'])
                    sizes.append(tmpVar['fsize'])
                    checksums.append(tmpVar['chksum'])
                # set dis dataset
                tmpFileSpec.dispatchDBlock = dispatchDBlock
            # register and subscribe dis dataset
            if len(lfns) != 0:
                # set dis dataset
                jumboJobSpec.dispatchDBlock = dispatchDBlock
                # register dis dataset
                try:
                    self.logger.debug('registering jumbo dis dataset {0} with {1} files'.format(dispatchDBlock,
                                                                                                len(lfns)))
                    out = rucioAPI.registerDataset(dispatchDBlock,lfns,guids,sizes,
                                                   checksums,lifetime=14)
                    vuid = out['vuid']
                    rucioAPI.closeDataset(dispatchDBlock)
                except:
                    errType,errValue = sys.exc_info()[:2]
                    self.logger.debug('failed to register jumbo dis dataset {0} with {1}:{2}'.format(dispatchDBlock,
                                                                                                     errType,errValue))
                    jumboJobSpec.jobStatus = 'failed'
                    jumboJobSpec.ddmErrorCode = ErrorCode.EC_Setupper
                    jumboJobSpec.ddmErrorDiag = 'failed to register jumbo dispatch dataset {0}'.format(dispatchDBlock)
                    ngJobs.append(jumboJobSpec)
                    continue
                # subscribe dis dataset
                try:
                    endPoint = self.siteMapper.getSite(jumboJobSpec.computingSite).ddm_input
                    self.logger.debug('subscribing jumbo dis dataset {0} to {1}'.format(dispatchDBlock,endPoint))
                    rucioAPI.registerDatasetSubscription(dispatchDBlock,[endPoint],lifetime=14,activity='Production Input')
                except:
                    errType,errValue = sys.exc_info()[:2]
                    self.logger.debug('failed to subscribe jumbo dis dataset {0} to {1} with {2}:{3}'.format(dispatchDBlock,
                                                                                                             endPoint,
                                                                                                             errType,errValue))
                    jumboJobSpec.jobStatus = 'failed'
                    jumboJobSpec.ddmErrorCode = ErrorCode.EC_Setupper
                    jumboJobSpec.ddmErrorDiag = 'failed to subscribe jumbo dispatch dataset {0} to {1}'.format(dispatchDBlock,
                                                                                                               endPoint)
                    ngJobs.append(jumboJobSpec)
                    continue
                # add dataset in DB
                ds = DatasetSpec()
                ds.vuid = vuid
                ds.name = dispatchDBlock
                ds.type = 'dispatch'
                ds.status = 'defined'
                ds.numberfiles  = len(lfns)
                ds.currentfiles = 0
                self.taskBuffer.insertDatasets([ds])
            # set destination
            jumboJobSpec.destinationSE = jumboJobSpec.computingSite
            for tmpFileSpec in jumboJobSpec.Files:
                if tmpFileSpec.type in ['output','log'] and \
                        DataServiceUtils.getDistributedDestination(tmpFileSpec.destinationDBlockToken) == None:
                    tmpFileSpec.destinationSE = jumboJobSpec.computingSite
            okJobs.append(jumboJobSpec)
        # update failed jobs
        self.updateFailedJobs(ngJobs)
        self.jumboJobs = okJobs
        self.logger.debug('done for jumbo jobs')
        return
