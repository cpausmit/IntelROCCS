//==================================================================================================
//
// Plot dataset characteristics as reported in the local popularity database cache.
//
//
//==================================================================================================
#include <iostream>
#include <fstream>

#include <TROOT.h>
#include <TSystem.h>
#include <TStyle.h>
#include <TString.h>
#include <TCanvas.h>
#include <TGraph.h>
#include <TMultiGraph.h>
#include <TH1D.h>
#include <TLegend.h>
#include <TText.h>
using namespace std;

void plotDatasetUsage();

//--------------------------------------------------------------------------------------------------
void plotDatasets()
{
  TString  styleMacro = gSystem->Getenv("MIT_ROOT_STYLE");
  long int rc = gROOT->LoadMacro(styleMacro+"+");
  printf(" Return code of loading styles: %d\n",rc);

  plotDatasetUsage();
}

//--------------------------------------------------------------------------------------------------
void plotDatasetUsage()
{
  TString text        = gSystem->Getenv("DATASET_MONITOR_TEXT");
  TString fileName    = gSystem->Getenv("DATASET_MONITOR_FILE");

  TString pngFileName = fileName + TString(".png");    
  TString inputFile   = fileName + TString(".txt");

  // Make sure we have the right styles
  MitRootStyle::Init();

  // Now open our database output
  ifstream input;
  input.open(inputFile.Data());

  Int_t    nLines=0;
  Double_t totalSize=0;

  Int_t    nSites=0, nFiles=0, nAccesses=0;
  Double_t size=0;

  Double_t xMin=0, xMax=40;

  // book our histogram
  TH1D *h = new TH1D("dataUsage","Data Usage",int(xMax-xMin),xMin,xMax);
  MitRootStyle::InitHist(h,"","",kBlack);  
  TString titles = TString("; Accesses ") + text + TString(";Data Size [TB]");
  h->SetTitle(titles.Data());

  // Loop over the file
  //-------------------
  while (1) {

    // read in 
    input >> nSites >> nAccesses >> nFiles >> size;

    // check it worked
    if (! input.good())
      break;
    
    // show what we are reading
    if (nLines < 5)
      printf(" nSites=%d, size=%8f nFiles=%d  nAccesses=%d\n",nSites, size, nFiles, nAccesses);

    Double_t value = double(nAccesses)/double(nFiles*nSites), weight = double(nSites)*size/1024.;

    totalSize += double(nSites)*size/1024.;

    // truncate at maximum (overflow in last bin) 
    if (value>=xMax)
      value = xMax-0.0001;
    
    h->Fill(value,weight);
    
    nLines++;
  }
  input.close();

  printf(" \n");
  printf(" Found %d entries.\n",nLines);
  printf(" Found %.3f [PB] total volume.\n",totalSize/1024.);
  printf(" \n");

  // Open a canvas
  TCanvas *cv = new TCanvas();
  cv->Draw();
  h->Draw("hist");

  MitRootStyle::OverlayFrame();
  MitRootStyle::AddText("Overflow in last bin.");

  cv->SaveAs(pngFileName.Data());
}
