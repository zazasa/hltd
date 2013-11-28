import FWCore.ParameterSet.Config as cms
import FWCore.ParameterSet.VarParsing as VarParsing

options = VarParsing.VarParsing ('analysis')

options.register ('runNumber',
                  1, # default value
                  VarParsing.VarParsing.multiplicity.singleton,
                  VarParsing.VarParsing.varType.int,          # string, int, or float
                  "Run Number")

options.register ('buBaseDir',
                  '/bu/', # default value
                  VarParsing.VarParsing.multiplicity.singleton,
                  VarParsing.VarParsing.varType.string,          # string, int, or float
                  "BU base directory")

options.parseArguments()

process = cms.Process("FAKEBU")
process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(-1)
)

process.options = cms.untracked.PSet(
    multiProcesses = cms.untracked.PSet(
    maxChildProcesses = cms.untracked.int32(0)
    )
)

process.options = cms.untracked.PSet(
    multiProcesses = cms.untracked.PSet(
    maxChildProcesses = cms.untracked.int32(0)
    )
)
process.MessageLogger = cms.Service("MessageLogger",
                                    destinations = cms.untracked.vstring( 'cout' ),
                                    cout = cms.untracked.PSet(
    FwkReport = cms.untracked.PSet(
    reportEvery = cms.untracked.int32(1000),
    optionalPSet = cms.untracked.bool(True),
    limit = cms.untracked.int32(10000000)
    ),
    threshold = cms.untracked.string( "INFO" ),
    )
                                    )


process.source = cms.Source("EmptySource",
     firstRun= cms.untracked.uint32(options.runNumber),
     numberEventsInLuminosityBlock = cms.untracked.uint32(2000),
     numberEventsInRun       = cms.untracked.uint32(0)    
)



process.EvFDaqDirector = cms.Service("EvFDaqDirector",
                                     baseDir = cms.untracked.string("/data"),
                                     buBaseDir = cms.untracked.string("/data"),
                                     hltBaseDir = cms.untracked.string("/data"),
                                     smBaseDir  = cms.untracked.string("/data/sm"),
                                     directorIsBu = cms.untracked.bool(True),
                                     slaveResources = cms.untracked.vstring('dvfu-c2f37-38-01', 'dvfu-c2f37-38-02','dvfu-c2f37-38-03','dvfu-c2f37-38-04'),
                                     #slaveResources = cms.untracked.vstring('dvfu-c2f37-38-01'),
                                     slavePathToData = cms.untracked.string("/data")
                                     )
process.EvFBuildingThrottle = cms.Service("EvFBuildingThrottle",
                                          highWaterMark = cms.untracked.double(0.50),
                                          lowWaterMark = cms.untracked.double(0.45)
                                          )

process.s = cms.EDProducer("DaqFakeReader")

process.a = cms.EDAnalyzer("ExceptionGenerator",
                           defaultAction = cms.untracked.int32(0),
                           defaultQualifier = cms.untracked.int32(10)
                           )

process.out = cms.OutputModule("RawStreamFileWriterForBU",
                               ProductLabel = cms.untracked.string("s"),
                               numWriters = cms.untracked.uint32(1),
			       eventBufferSize = cms.untracked.uint32(100),
   			       jsonDefLocation = cms.untracked.string("/nfshome0/aspataru/cmssw/CMSSW_6_2_0_pre3/src/EventFilter/Utilities/plugins/budef.jsd"),
			       #lumiSubdirectoriesMode=cms.untracked.bool(False),
			       debug = cms.untracked.bool(True)
                               )

process.p = cms.Path(process.s+process.a)

process.ep = cms.EndPath(process.out)
