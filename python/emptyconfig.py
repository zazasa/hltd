import FWCore.ParameterSet.Config as cms

process = cms.Process('HLT')
# Input source
process.source = cms.Source("EmptySource")
process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(-1),
    output = cms.untracked.int32(-1)
)

process.a = cms.EDAnalyzer("ExceptionGenerator",
             defaultAction       = cms.untracked.int32(0),
             defaultQualifier    = cms.untracked.int32(1000)
)
process.p = cms.Path(process.a)
