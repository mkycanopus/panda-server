'''
setup dataset for ATLAS

'''

import re
import sys
import time
import uuid
import types
import urllib
import hashlib
import datetime
import commands
import threading
import traceback
import ErrorCode
from DDM import rucioAPI
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from taskbuffer.DatasetSpec import DatasetSpec
from taskbuffer import retryModule
from taskbuffer import EventServiceUtils
from brokerage.SiteMapper import SiteMapper
from brokerage.PandaSiteIDs import PandaMoverIDs
import brokerage.broker
import DataServiceUtils
from rucio.client import Client as RucioClient
from rucio.common.exception import FileAlreadyExists,DataIdentifierAlreadyExists,Duplicate,\
    DataIdentifierNotFound

from SetupperPluginBase import SetupperPluginBase

from config import panda_config


class SetupperAtlasPlugin (SetupperPluginBase):
    # constructor
    def __init__(self,taskBuffer,jobs,logger,**params):
        # defaults
        defaultMap = {'resubmit'      : False,
                      'pandaDDM'      : False,
                      'ddmAttempt'    : 0,
                      'resetLocation' : False,
                      'useNativeDQ2'  : True,
                     }
        SetupperPluginBase.__init__(self,taskBuffer,jobs,logger,params,defaultMap)
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
        
        
    # main
    def run(self):
        try:
            self.logger.debug('start run()')
            self._memoryCheck()
            bunchTag = ''
            timeStart = datetime.datetime.utcnow()
            if self.jobs != None and len(self.jobs) > 0:
                bunchTag = 'PandaID:%s type:%s taskID:%s pType=%s' % (self.jobs[0].PandaID,
                                                                      self.jobs[0].prodSourceLabel,
                                                                      self.jobs[0].taskID,
                                                                      self.jobs[0].processingType)
                self.logger.debug(bunchTag)
            # instantiate site mapper
            self.siteMapper = SiteMapper(self.taskBuffer)
            # correctLFN
            self._correctLFN()

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
            if self.jobs != [] and self.jobs[0].prodSourceLabel in ['managed','test']:
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
            regTime = datetime.datetime.utcnow() - timeStart
            self.logger.debug('{0} took {1}sec'.format(bunchTag,regTime.seconds))
            self.logger.debug('end run()')
        except:
            errtype, errvalue = sys.exc_info()[:2]
            errStr = "run() : %s %s" % (errtype, errvalue)
            errStr.strip()
            errStr += traceback.format_exc()
            self.logger.error(errStr)

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
        This function creates the dispatch datasets, resolves the files, and registers them in Rucio and PanDA database.
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
                        newOut,errMsg = rucioAPI.listDatasets(job.prodDBlock)
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
                nDDMTry = 3
                isOK = False
                errStr = ''
                for iDDMTry in range(nDDMTry):
                    try:
                        out = rucioAPI.registerDataset(dispatch_block, disFiles['lfns'], disFiles['guids'],
                                                       disFiles['fsizes'], disFiles['chksums'],
                                                       lifetime=7, scope='panda', metadata=metadata)
                        isOK = True
                        break
                    except:
                        errType, errValue = sys.exc_info()[:2]
                        errStr = "{0}:{1}".format(errType,errValue)
                        self.logger.error("registerDataset : failed with {0}".format(errStr))
                        if iDDMTry+1 == nDDMTry:
                            break
                        self.logger.debug("sleep {0}/{1}".format(iDDMTry, nDDMTry))
                        time.sleep(10)

                if not isOK:
                    # registration failed
                    dispatch_ds_errors[dispatch_block] = "Setupper._setupSource() could not register dispatchDBlock with {0}".format(errStr.split('\n')[-1])
                    continue
                self.logger.debug(out)
                newOut = out

                # freeze dispatch dataset
                self.logger.debug('closeDataset {0}'.format(dispatch_block))
                for iDDMTry in range(3):
                    status = False
                    try:
                        rucioAPI.closeDataset(dispatch_block)
                        status = True
                        break
                    except:
                        errtype, errvalue = sys.exc_info()[:2]
                        out = 'failed to close : {0} {1}'.format(errtype, errvalue)
                        time.sleep(10)
                if not status:
                    self.logger.error(out)
                    dispatch_ds_errors[dispatch_block] = "Setupper._setupSource() could not freeze dispatchDBlock with {0}".format(out)
                    continue
            else:
                # use PandaDDM
                self.dispatch_file_map[dispatch_block] = file_list[dispatch_block]
                # create a fake vuid
                newOut = {'vuid': str(uuid.uuid4())}

            # get VUID
            try:
                vuid = newOut['vuid']
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
                errtype,errvalue = sys.exc_info()[:2]
                self.logger.error("_setupSource() : %s %s" % (errtype,errvalue))
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
        Create datasets for outputs in the repository and assign destination
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

            # ignore failed jobs
            if job.jobStatus in ['failed','cancelled'] or job.isCancelled():
                continue

            zip_file_map = job.getZipFileMap()
            for file in job.Files:
                # ignore input files
                if file.type in ['input','pseudo_input']:
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
                                    and ds_name != original_name and re.search('_sub\d+$',ds_name) != None:
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
                                            re.search('_sub\d+$',ds_name) != None and \
                                            (not job.prodSourceLabel in ['user','panda']) and \
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
                                elif (not usingT1asT2) and (not file.destinationDBlockToken in ['NULL','']):
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
                                isOK = False
                                for iDDMTry in range(3):
                                    try:
                                        out = rucioAPI.registerDataset(ds_name,metadata=tmpMetadata,
                                                                       lifetime=tmpLifeTime)
                                        self.logger.debug(out)
                                        newVUID = out['vuid']
                                        isOK = True
                                        break
                                    except:
                                        errType,errValue = sys.exc_info()[:2]
                                        self.logger.error("registerDataset : failed with {0}:{1}".format(errType,errValue))
                                        time.sleep(10)
                                if not isOK:
                                    tmpMsg = "Setupper._setupDestination() could not register : %s" % ds_name
                                    dest_error[dest] = tmpMsg
                                    self.logger.error(tmpMsg)
                                    continue

                                # register dataset locations
                                if (job.lockedby == 'jedi' and job.prodSourceLabel in ['panda','user']) or \
                                        DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) != None:
                                    # skip registerDatasetLocations
                                    status,out = True, ''
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

                                        for iDDMTry in range(3):
                                            try:
                                                out = rucioAPI.registerDatasetLocation(ds_name,[dq2ID],lifetime=repLifeTime,
                                                                                       activity=activity,
                                                                                       grouping=grouping)
                                                self.logger.debug(out)
                                                status = True
                                                break
                                            except:
                                                errType,errValue = sys.exc_info()[:2]
                                                out = "{0}:{1}".format(errType,errValue)
                                                self.logger.error("registerDatasetLocation : failed with {0}".format(out))
                                                time.sleep(10)
                                        # failed
                                        if not status:
                                            break
                                else:
                                    # skip registerDatasetLocations
                                    status,out = True,''
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
                            ds.vuid         = newVUID
                            ds.name         = ds_name
                            ds.type         = 'output'
                            ds.numberfiles  = 0
                            ds.currentfiles = 0
                            ds.status       = 'defined'
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

                # destination endpoints
                dst_sitespec = self.siteMapper.getSite(job.computingSite)
                dst_endpoint = dst_sitespec.ddm_input # TODO: here you need the better logic of a distributed dataset
                                                       # TODO: is there any precaution to take in case of staging?

                # use DDM/Rucio
                if (not self.pandaDDM) and job.prodSourceLabel != 'ddm':

                    # set activity and owner
                    if job.prodSourceLabel in ['user', 'panda']:
                        opt_activity = "Analysis Input"
                        #opt_owner = DataServiceUtils.cleanupDN(job.prodUserID)
                        opt_owner = None
                    else:
                        opt_owner = None
                        if job.processingType == 'urgent' or job.currentPriority > 1000:
                            opt_activity = 'Express'
                        else:
                            opt_activity = "Production Input"

                    # taskID
                    if job.jediTaskID not in ['NULL',0]:
                        optComment = 'task_id:{0}'.format(job.jediTaskID)
                    else:
                        optComment = None

                    # TODO: this part needs the Rucio distributed dataset logic
                    # http://rucio.readthedocs.io/en/latest/api/rule.html
                    # register subscription
                    self.logger.debug('{0} {1} {2}'.format('registerDatasetSubscription',
                                                           (job.dispatchDBlock, dst_endpoint),
                                                           {'activity': opt_activity, 'lifetime': 7,
                                                            'dn': opt_owner, 'comment': optComment}))
                    for iDDMTry in range(3):
                        try:
                            status = rucioAPI.registerDatasetSubscription(job.dispatchDBlock, [dst_endpoint],
                                                                          activity=opt_activity,
                                                                          lifetime=7, dn=opt_owner,
                                                                          comment=optComment)
                            out = 'OK'
                            break
                        except:
                            status = False
                            err_type, err_value = sys.exc_info()[:2]
                            out = "%s %s" % (err_type, err_value)
                            time.sleep(10)
                    if not status:
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

    # correct LFN for attemptNr
    def _correctLFN(self):

        lfnMap = {}
        valMap = {}
        input_ds_errors = {}
        missingDS = {}
        jobsWaiting = []
        jobsFailed = []
        jobsProcessed = []
        allLFNs   = {}
        allGUIDs  = {}
        allScopes = {}
        cloudMap  = {}
        lfnDsMap  = {}
        replicaMap = {}

        self.logger.debug('go into LFN correction')

        # collect input LFNs
        inputLFNs = set()
        for tmpJob in self.jobs:
            for tmpFile in tmpJob.Files:
                if tmpFile.type == 'input':
                    inputLFNs.add(tmpFile.lfn)
                    genLFN = re.sub('\.\d+$','',tmpFile.lfn)
                    inputLFNs.add(genLFN)
                    if not tmpFile.GUID in ['NULL','',None]:
                        if not tmpFile.dataset in self.lfnDatasetMap:
                            self.lfnDatasetMap[tmpFile.dataset] = {}
                        self.lfnDatasetMap[tmpFile.dataset][tmpFile.lfn] = {'guid': tmpFile.GUID,
                                                                            'chksum': tmpFile.checksum,
                                                                            'md5sum': tmpFile.md5sum,
                                                                            'fsize': tmpFile.fsize,
                                                                            'scope': tmpFile.scope}

        # loop over all jobs to collect datasets, resolve the files and create a dataset replica map
        for job in self.jobs:
            # set job failed if the computingSite does not exist
            if job.computingSite != 'NULL' and (not job.computingSite in self.siteMapper.siteSpecList.keys()):
                job.jobStatus    = 'failed'
                job.ddmErrorCode = ErrorCode.EC_Setupper                
                job.ddmErrorDiag = "computingSite:%s is unknown" % job.computingSite
                # append job for downstream process
                jobsProcessed.append(job)
                continue

            # ignore no prodDBlock jobs or container dataset
            if job.prodDBlock == 'NULL':
                # append job to processed list
                jobsProcessed.append(job)
                continue

            # collect datasets
            datasets = []
            for file in job.Files:
                if file.type == 'input' and file.dispatchDBlock == 'NULL' \
                        and (file.GUID == 'NULL' or job.prodSourceLabel in ['managed', 'test', 'ptest']):
                    if file.dataset not in datasets:
                        datasets.append(file.dataset)

            # get LFN list
            for dataset in datasets:
                if not dataset in lfnMap.keys():
                    input_ds_errors[dataset] = ''
                    lfnMap[dataset] = {}
                    # get LFNs
                    status, file_list = self.getListFilesInDataset(dataset, inputLFNs)
                    if status != 0:
                        self.logger.error(out)
                        input_ds_errors[dataset] = 'could not get file list of prodDBlock %s' % dataset
                        self.logger.error(input_ds_errors[dataset])
                        # doesn't exist in Rucio
                        if status == -1:
                            missingDS[dataset] = "DS:%s not found in DDM" % dataset
                        else:
                            missingDS[dataset] = out
                    else:
                        # make map (key: LFN w/o attemptNr, value: LFN with attemptNr)
                        file_list = out
                        try:
                            # loop over all files    
                            for tmpLFN, vals in file_list.iteritems():
                                valMap[tmpLFN] = vals
                                genLFN = re.sub('\.\d+$','',tmpLFN)
                                if lfnMap[dataset].has_key(genLFN):
                                    # get attemptNr
                                    newAttNr = 0
                                    newMat = re.search('\.(\d+)$', tmpLFN)
                                    if newMat != None:
                                        newAttNr = int(newMat.group(1))
                                    oldAttNr = 0
                                    oldMat = re.search('\.(\d+)$', lfnMap[dataset][genLFN])
                                    if oldMat != None:
                                        oldAttNr = int(oldMat.group(1))
                                    # compare
                                    if newAttNr > oldAttNr:
                                        lfnMap[dataset][genLFN] = tmpLFN
                                else:
                                    lfnMap[dataset][genLFN] = tmpLFN
                                # mapping from LFN to DS
                                lfnDsMap[lfnMap[dataset][genLFN]] = dataset
                        except:
                            input_ds_errors[dataset] = 'could not convert HTTP-res to map for prodDBlock %s' % dataset
                            self.logger.error(input_ds_errors[dataset])
                            self.logger.error(out)

                    # get replica locations
                    if job.prodSourceLabel in ['managed','test'] and input_ds_errors[dataset] == '' \
                            and (not replicaMap.has_key(dataset)):
                        if dataset.endswith('/'):
                            status, out = self.getListDatasetReplicasInContainer(dataset)
                        else:
                            status, out = self.getListDatasetReplicas(dataset,False)

                        if status != 0 or out.startswith('Error'):
                            input_ds_errors[dataset] = 'could not get locations for %s' % dataset
                            self.logger.error(input_ds_errors[dataset])
                            self.logger.error(out)
                        else:
                            tmp_replicas_sites = {}
                            try:
                                # convert res to map
                                exec "tmp_replicas_sites = %s" % out
                                replicaMap[dataset] = tmp_replicas_sites
                            except:
                                input_ds_errors[dataset] = 'could not convert HTTP-res to replica map for %s' % dataset
                                self.logger.error(input_ds_errors[dataset])
                                self.logger.error(out)

                            # append except DBR
                            if not dataset.startswith('ddo'):
                                self.replicaMapForBroker[dataset] = tmp_replicas_sites

            # check for missing datasets and set their files as missing
            isFailed = False
            for dataset in datasets:
                if missingDS.has_key(dataset):
                    job.jobStatus = 'failed'
                    job.ddmErrorCode = ErrorCode.EC_GUID
                    job.ddmErrorDiag = missingDS[dataset]
                    # set missing
                    for tmpFile in job.Files:
                        if tmpFile.dataset == dataset:
                            tmpFile.status = 'missing'
                    # append        
                    jobsFailed.append(job)
                    isFailed = True

                    self.logger.debug("{0} failed with {1}".format(job.PandaID, missingDS[dataset]))
                    break
            if isFailed:
                continue

            # check for waiting
            for dataset in datasets:
                if input_ds_errors[dataset] != '':
                    # append job to waiting list
                    jobsWaiting.append(job)
                    isFailed = True
                    break
            if isFailed:
                continue

            # replace generic LFN with real LFN
            replaceList = []
            isFailed = False
            for file in job.Files:
                if file.type == 'input' and file.dispatchDBlock == 'NULL':
                    addToLfnMap = True
                    if file.GUID == 'NULL':
                        # get LFN w/o attemptNr
                        basename = re.sub('\.\d+$','',file.lfn)
                        if basename == file.lfn:
                            # replace
                            if basename in lfnMap[file.dataset].keys():
                                file.lfn = lfnMap[file.dataset][basename]
                                replaceList.append((basename,file.lfn))
                        # set GUID
                        if file.lfn in valMap:
                            file.GUID = valMap[file.lfn]['guid']
                            file.fsize = valMap[file.lfn]['fsize']
                            file.md5sum = valMap[file.lfn]['md5sum']
                            file.checksum = valMap[file.lfn]['chksum']
                            file.scope = valMap[file.lfn]['scope']
                            # remove white space
                            if file.md5sum is not None:
                                file.md5sum = file.md5sum.strip()
                            if file.checksum is not None:
                                file.checksum = file.checksum.strip()
                    else:
                        if not job.prodSourceLabel in ['managed','test']:
                            addToLfnMap = False

                    # check missing file
                    if file.GUID == 'NULL' or job.prodSourceLabel in ['managed','test']:
                        if not file.lfn in valMap:
                            # append job to waiting list
                            errMsg = "GUID for {0} not found in Rucio".format(file.lfn)
                            self.logger.error(errMsg)
                            file.status = 'missing'
                            if not job in jobsFailed:
                                job.jobStatus = 'failed'
                                job.ddmErrorCode = ErrorCode.EC_GUID
                                job.ddmErrorDiag = errMsg
                                jobsFailed.append(job)
                                isFailed = True
                            continue

                    # add to allLFNs/allGUIDs
                    if addToLfnMap:
                        if not allLFNs.has_key(job.getCloud()):
                            allLFNs[job.getCloud()] = []
                        if not allGUIDs.has_key(job.getCloud()):
                            allGUIDs[job.getCloud()] = []
                        if not allScopes.has_key(job.getCloud()):
                            allScopes[job.getCloud()] = []
                        allLFNs[job.getCloud()].append(file.lfn)
                        allGUIDs[job.getCloud()].append(file.GUID)
                        allScopes[job.getCloud()].append(file.scope)

            # modify jobParameters
            if not isFailed:
                for patt, repl in replaceList:
                    job.jobParameters = re.sub('%s '% patt, '%s '% repl, job.jobParameters)
                # append job to processed list
                jobsProcessed.append(job)

        # TODO: there is some part to fill the missing files at the computingSite that needs to be written

        # splits the dataset name to retrieve summary fields such as project and filetype
        for tmpJob in self.jobs:
            try:
                # set only for production/analysis/test
                if not tmpJob.prodSourceLabel in ['managed', 'test', 'rc_test', 'ptest', 'user', 'prod_test']:
                    continue

                # loop over all files
                tmpJob.nInputDataFiles = 0
                tmpJob.inputFileBytes = 0
                tmpInputFileProject = None
                tmpInputFileType = None
                for tmpFile in tmpJob.Files:
                    # use input files and ignore DBR/lib.tgz
                    if tmpFile.type == 'input' and (not tmpFile.dataset.startswith('ddo')) and not tmpFile.lfn.endswith('.lib.tgz'):
                        tmpJob.nInputDataFiles += 1
                        if not tmpFile.fsize in ['NULL',None,0,'0']:
                            tmpJob.inputFileBytes += tmpFile.fsize

                        # get input type and project
                        if tmpInputFileProject is None:
                            tmpInputItems = tmpFile.dataset.split('.')
                            # input project
                            tmpInputFileProject = tmpInputItems[0].split(':')[-1]
                            # input type. ignore user/group/groupXY 
                            if len(tmpInputItems) > 4 and (not tmpInputItems[0] in ['', 'NULL', 'user', 'group']) \
                                   and (not tmpInputItems[0].startswith('group')) and not tmpFile.dataset.startswith('panda.um.'):
                                tmpInputFileType = tmpInputItems[4]

                # set input type and project
                if not tmpJob.prodDBlock in ['', None, 'NULL']:
                    # input project
                    if tmpInputFileProject != None:
                        tmpJob.inputFileProject = tmpInputFileProject
                    # input type
                    if tmpInputFileType != None:
                        tmpJob.inputFileType = tmpInputFileType

                # protection
                maxInputFileBytes = 99999999999
                if tmpJob.inputFileBytes > maxInputFileBytes:
                    tmpJob.inputFileBytes = maxInputFileBytes
                # set background-able flag
                tmpJob.setBackgroundableFlag()

            except:
                errType,errValue = sys.exc_info()[:2]
                self.logger.error("failed to set data summary fields for PandaID=%s: %s %s" % (tmpJob.PandaID,errType,errValue))

        # send jobs to jobsWaiting4 table
        self.taskBuffer.keepJobs(jobsWaiting)
        # update failed job
        self.updateFailedJobs(jobsFailed)
        # remove waiting/failed jobs
        self.jobs = jobsProcessed

        # delete huge variables
        del lfnMap
        del valMap
        del input_ds_errors
        del jobsWaiting
        del jobsProcessed
        del allLFNs
        del allGUIDs
        del cloudMap

    def removeWaitingJobs(self):
        """
        Iterate self.jobs and put waiting jobs into a separate list
        """
        jobs_waiting = []
        jobs_processed = []

        # iterate jobs and separate waiting jobs
        for tmpJob in self.jobs:
            if tmpJob.jobStatus == 'waiting':
                jobs_waiting.append(tmpJob)
            else:
                jobs_processed.append(tmpJob)

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
                self.logger.debug('listFilesInDataset '+dataset)
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
            status,out = rucioAPI.listDatasetReplicas(dataset)
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
        Make dispatch datasets for existing files to avoid deletion while jobs are queued
        # TODO: what is the difference with SetupSource?
        :return:
        """

        self.logger.debug('make dis datasets for existing files')
        # collect existing files
        dsFileMap = {}
        nMaxJobs  = 20
        nJobsMap  = {}

        for tmpJob in self.jobs:

            # use production or test jobs only
            if tmpJob.prodSourceLabel not in ['managed', 'test']:
                continue
            # skip for prefetcher or transferType=direct
            if tmpJob.usePrefetcher() or tmpJob.transferType == 'direct':
                continue
            # ignore inappropriate status
            if tmpJob.jobStatus in ['failed', 'cancelled', 'waiting'] or tmpJob.isCancelled():
                continue

            # TODO: what is the background for this ND condition???
            # check cloud
            if tmpJob.getCloud() == 'ND' and self.siteMapper.getSite(tmpJob.computingSite).cloud == 'ND':
                continue

            # check SE to use T2 only
            tmpSrcID = self.siteMapper.getCloud(tmpJob.getCloud())['source']
            srcSiteSpec = self.siteMapper.getSite(tmpSrcID)
            dstSiteSpec = self.siteMapper.getSite(tmpJob.computingSite)
            if dstSiteSpec.ddm_endpoints_input.isAssociated(srcSiteSpec.ddm_input):
                continue

            # look for log _sub dataset to be used as a key
            logSubDsName = ''
            for tmpFile in tmpJob.Files:
                if tmpFile.type == 'log':
                    logSubDsName = tmpFile.destinationDBlock
                    break

            destDQ2ID = self.siteMapper.getSite(tmpJob.computingSite).ddm_input

            # backend
            ddmBackEnd = 'rucio'
            mapKeyJob = (destDQ2ID, logSubDsName)
            mapKey = (destDQ2ID, logSubDsName, nJobsMap[mapKeyJob] / nMaxJobs, ddmBackEnd)

            # increment the number of jobs per key
            nJobsMap.setdefault(mapKeyJob, 0)
            nJobsMap[mapKeyJob] += 1

            dsFileMap.setdefault(mapKey, {})

            # add files
            for tmpFile in tmpJob.Files:
                if tmpFile.type != 'input':
                    continue

                # if files are unavailable at the dest site normal dis datasets contain them
                # or files are cached
                if not tmpFile.status in ['ready']:
                    continue

                # if available at T2
                # TODO: understand how this works
                realDestDQ2ID = (destDQ2ID, )
                if self.availableLFNsInT2.has_key(tmpJob.getCloud()) and self.availableLFNsInT2[tmpJob.getCloud()].has_key(tmpFile.dataset) \
                   and self.availableLFNsInT2[tmpJob.getCloud()][tmpFile.dataset]['sites'].has_key(tmpJob.computingSite) \
                   and tmpFile.lfn in self.availableLFNsInT2[tmpJob.getCloud()][tmpFile.dataset]['sites'][tmpJob.computingSite]:
                    realDestDQ2ID = self.availableLFNsInT2[tmpJob.getCloud()][tmpFile.dataset]['siteDQ2IDs'][tmpJob.computingSite]
                    realDestDQ2ID = tuple(realDestDQ2ID)

                if not dsFileMap[mapKey].has_key(realDestDQ2ID):
                    dsFileMap[mapKey][realDestDQ2ID] = {'taskID': tmpJob.taskID,
                                                        'PandaID': tmpJob.PandaID,
                                                        'files': {}}

                if not dsFileMap[mapKey][realDestDQ2ID]['files'].has_key(tmpFile.lfn):
                    # add scope
                    tmpLFN = '{0}:{1}'.format(tmpFile.scope, tmpFile.lfn)
                    dsFileMap[mapKey][realDestDQ2ID]['files'][tmpFile.lfn] = {'lfn' :tmpLFN,
                                                                              'guid':tmpFile.GUID,
                                                                              'fileSpecs':[]}
                # add file spec
                dsFileMap[mapKey][realDestDQ2ID]['files'][tmpFile.lfn]['fileSpecs'].append(tmpFile)

        # loop over all locations
        dispList = []
        for tmpMapKey,tmpDumVal in dsFileMap.iteritems():
            tmpDumLocation, tmpLogSubDsName, tmpBunchIdx, tmpDdmBackEnd = tmpMapKey
            for tmpLocationList,tmpVal in tmpDumVal.iteritems():
                for tmpLocation in tmpLocationList:
                    tmpfile_list = tmpVal['files']
                    if tmpfile_list == {}:
                        continue
                    nMaxFiles = 500
                    iFiles = 0
                    iLoop = 0
                    while iFiles < len(tmpfile_list):
                        subFileNames = tmpfile_list.keys()[iFiles:iFiles+nMaxFiles]
                        if len(subFileNames) == 0:
                            break
                        # dis name
                        disDBlock = "panda.%s.%s.%s.%s_dis0%s%s" % (tmpVal['taskID'], time.strftime('%m.%d'), 'GEN',
                                                                    commands.getoutput('uuidgen'), iLoop,
                                                                    tmpVal['PandaID'])
                        iFiles += nMaxFiles
                        lfns    = []
                        guids   = []
                        fsizes  = []
                        chksums = []

                        for tmpSubFileName in subFileNames:
                            lfns.append(tmpfile_list[tmpSubFileName]['lfn'])
                            guids.append(tmpfile_list[tmpSubFileName]['guid'])
                            fsizes.append(long(tmpfile_list[tmpSubFileName]['fileSpecs'][0].fsize))
                            chksums.append(tmpfile_list[tmpSubFileName]['fileSpecs'][0].checksum)
                            # set dis name
                            for tmpFileSpec in tmpfile_list[tmpSubFileName]['fileSpecs']:
                                if tmpFileSpec.status in ['ready'] and tmpFileSpec.dispatchDBlock == 'NULL':
                                    tmpFileSpec.dispatchDBlock = disDBlock

                        # register datasets
                        iLoop += 1
                        nDDMTry = 3
                        isOK = False
                        metadata = {'hidden':True,
                                    'purge_replicas': 0}
                        if not tmpVal['taskID'] in [None,'NULL']:
                            metadata['task_id'] = str(tmpVal['taskID'])
                        tmpMsg = 'ext registerNewDataset {ds} {lfns} {guids} {fsizes} {chksums} {meta}'
                        self.logger.debug(tmpMsg.format(ds=disDBlock,
                                                        lfns=str(lfns),
                                                        guids=str(guids),
                                                        fsizes=str(fsizes),
                                                        chksums=str(chksums),
                                                        meta=str(metadata)))
                        for iDDMTry in range(nDDMTry):
                            try:
                                out = rucioAPI.registerDataset(disDBlock, lfns, guids, fsizes, chksums,
                                                               lifetime=7, scope='panda', metadata=metadata)
                                self.logger.debug(out)
                                isOK = True
                                break
                            except:
                                errType,errValue = sys.exc_info()[:2]
                                self.logger.error("ext registerDataset : failed with {0}:{1}".format(errType,errValue)+traceback.format_exc())
                                if iDDMTry+1 == nDDMTry:
                                    break
                                self.logger.debug("sleep {0}/{1}".format(iDDMTry,nDDMTry))
                                time.sleep(10)
                        # failure
                        if not isOK:
                            continue
                        # get VUID
                        try:
                            exec "vuid = %s['vuid']" % str(out)
                            # dataset spec. currentfiles is used to count the number of failed jobs
                            ds = DatasetSpec()
                            ds.vuid = vuid
                            ds.name = disDBlock
                            ds.type = 'dispatch'
                            ds.status = 'defined'
                            ds.numberfiles  = len(lfns)
                            ds.currentfiles = 0
                            dispList.append(ds)
                        except:
                            errType,errValue = sys.exc_info()[:2]
                            self.logger.error("ext registerNewDataset : failed to decode VUID for %s - %s %s" % (disDBlock,errType,errValue))
                            continue

                        # freezeDataset dispatch dataset
                        self.logger.debug('freezeDataset '+disDBlock)
                        for iDDMTry in range(3):
                            status = False
                            try:
                                rucioAPI.closeDataset(disDBlock)
                                status = True
                                break
                            except:
                                errtype,errvalue = sys.exc_info()[:2]
                                out = 'failed to close : {0} {1}'.format(errtype,errvalue)
                                time.sleep(10)
                        if not status:
                            self.logger.error(out)
                            continue

                        # register location
                        isOK = False
                        self.logger.debug('ext registerDatasetLocation {ds} {dq2ID} {lifeTime}days asynchronous=True'.
                                          format(ds=disDBlock, dq2ID=tmpLocation, lifeTime=7))
                        nDDMTry = 3
                        for iDDMTry in range(nDDMTry):
                            try:
                                out = rucioAPI.registerDatasetLocation(disDBlock, [tmpLocation], 7,
                                                                       activity='Production Input',
                                                                       scope='panda', asynchronous=True,
                                                                       grouping='NONE')
                                self.logger.debug(out)
                                isOK = True
                                break
                            except:
                                errType,errValue = sys.exc_info()[:2]
                                self.logger.error("ext registerDatasetLocation : failed with {0}:{1}".format(errType,errValue))
                                if iDDMTry+1 == nDDMTry:
                                    break
                                self.logger.debug("sleep {0}/{1}".format(iDDMTry,nDDMTry))
                                time.sleep(10)
                                
                        # failure
                        if not isOK:
                            continue

        # insert datasets to DB
        self.taskBuffer.insertDatasets(dispList)
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
            for tmpFileSpec in jumboJobSpec.Files:
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
                    jumboJobSpec.jobStatus    = 'failed'
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
                    jumboJobSpec.jobStatus    = 'failed'
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
