import FWCore.ParameterSet.Config as cms

process = cms.Process("TESTFU")
process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(-1)
)

process.options = cms.untracked.PSet(
    multiProcesses = cms.untracked.PSet(
    maxChildProcesses = cms.untracked.int32(0)
    )
)
process.MessageLogger = cms.Service("MessageLogger",
                                    cout = cms.untracked.PSet(threshold = cms.untracked.string( "INFO" )
                                                              ),
                                    destinations = cms.untracked.vstring( 'cout' )
                                    )

process.FastMonitoringService = cms.Service("FastMonitoringService",
			  	    sleepTime = cms.untracked.int32(1),
				    rootDirectory = cms.untracked.string("/data/"),
				    definitionPath = cms.untracked.string( '/nfshome0/aspataru/cmssw/CMSSW_6_2_0_pre3/src/EventFilter/Utilities/plugins/microstatedef.jsd' ),
				    fastName = cms.untracked.string( 'states' ),
				    slowName = cms.untracked.string( 'lumi' )
				    )


process.source = cms.Source("FedRawDataInputSource",
		        rootFUDirectory = cms.untracked.string("/data/"),
		        rootBUDirectory = cms.untracked.string("/BU-RAMDISK/")
			)


process.a = cms.EDAnalyzer("ExceptionGenerator",
                           defaultAction = cms.untracked.int32(-1),
                           defaultQualifier = cms.untracked.int32(10)
                           )

process.p = cms.Path(process.a)



