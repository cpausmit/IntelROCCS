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
void plotDatasets(int average=0)
{
  TString  styleMacro = gSystem->Getenv("MIT_ROOT_STYLE");
  long int rc = gROOT->LoadMacro(styleMacro+"+");
  printf(" Return code of loading styles: %d\n",rc);

  plotDatasetUsage(average);
}

//--------------------------------------------------------------------------------------------------
void plotDatasetUsage(int average=0)
{
  TString text        = gSystem->Getenv("DATASET_MONITOR_TEXT");
  TString fileName    = gSystem->Getenv("DATASET_MONITOR_FILE");

  TString pngFileName = fileName + TString(".png");
  pngFileName = fileName + TString(".png"); // if we're using nSitesAv instead of nSites
  TString inputFile   = fileName + TString(".txt");

  // Make sure we have the right styles
  MitRootStyle::Init();

  // Now open our database output
  ifstream input;
  input.open(inputFile.Data());

  Int_t    nLines=0;
  Double_t totalSize=0;
  Double_t xMin=0, xMax=120;
  Double_t yMin=0, yMax=10;
  Int_t    nSites=0, nFiles=0, nAccesses=0;
  Double_t nSitesAv, size=0;
  TString  name;

  // book our histogram
  Int_t nBins = int(xMax-xMin);
  if(average==2) nBins = 20;
  Int_t nBinsy = 20;
  TH1D *h ; 
  TH2D *h2 ;
          if(average==2) h2 = new TH2D("dataUsage","Accesses vs TB",nBins,xMin,xMax,nBinsy,yMin,yMax);
          else h = new TH1D("dataUsage","Data Usage",nBins,xMin-0.5,xMax+0.5);
  if(average==2) MitRootStyle::InitHist(h2,"","",kBlack);
  else MitRootStyle::InitHist(h,"","",kBlack);
  TString titles = TString("; Accesses ") + text + TString(";Data Size [TB]");
  if(average==2) h2->SetTitle(titles.Data());
  else h->SetTitle(titles.Data());

  // Loop over the file
  //-------------------
  while (1) {

    // read in
    //input >> nSites >> nAccesses >> nFiles >> size;
    input >> nSites >> nSitesAv >> nAccesses >> nFiles >> size >> name;

    // check it worked
    if (! input.good())
      break;
    
    if (average==1) {
      // do we want nSites or nSitesAv
      // show what we are reading
      if (nLines < 5)
        printf(" nSitesAv=%.3f  nAccesses=%d  nFiles=%d  size=%8f\n",nSitesAv, nAccesses, nFiles, size);
    } else {
      // show what we are reading
      if (nLines < 5)
        printf(" nSites=%d  nAccesses=%d  nFiles=%d  size=%8f\n",nSites, nAccesses, nFiles, size);
    }
    Double_t value, valuey, weight;
    
    if (average==1) {
      value = double(nAccesses)/double(nFiles*nSitesAv); 
      weight = double(nSitesAv)*size/1024.;
    } else if (average==2) {
        value = double(nAccesses)/double(nFiles);
        valuey = double(size/1024.);
//        if (value) fprintf(stderr,"%i/(%i*%f) = %f\n",nAccesses,nFiles,size,value);
        weight = double(nSitesAv)*size/1024.;
    } else {  
      value = double(nAccesses)/double(nFiles*nSites); 
      weight = double(nSites)*size/1024.;
    }

    // treat the cases of few accesses:
    //
    // - to avoid confusing counts below zero pretend that accessing half of the files of
    //   one replica counts as one full access
    //   not desired behavior when plotting nAccesses/GB, usually < 1
    if ( value<1.0 && (double(nAccesses)/double(nFiles)>0.5))
      value = 1.;

    // truncate at maximum (overflow in last bin)
    if (value>=xMax)
      value = xMax-0.0001;
    if(average==2) {
       if(valuey>=yMax) value = yMax-0.0001;
    } 

    // fill the histogram here
    if(average==2) h2->Fill(value,valuey,weight);
    else h->Fill(value,weight);

    if (nLines < 5) {
      if(average==2) printf(" Value: %f,%f weight: %f\n",value,valuey,weight);
      else printf(" Value: %f weight: %f\n",value,weight);
    }

    // keep track of the total size
    if (average) totalSize += double(nSitesAv)*size/1024.;
    else totalSize += double(nSites)*size/1024.;

    // count the number of lines
    nLines++;
  }
  input.close();

  printf(" \n");
  printf(" Found %d entries.\n",nLines);
  printf(" Found %.3f [PB] total volume.\n",totalSize/1024.);
  printf(" \n");

  // Open a canvas
  TCanvas *cv = new TCanvas();
 // if (average==2) cv->SetLogy();
  cv->Draw();
  if(average==2) {
      h2->SetStats(0);
      h2->Draw("colz");
  }
    else   h->Draw("hist");

  MitRootStyle::OverlayFrame();
  MitRootStyle::AddText("Overflow in last bin.");

  cv->SaveAs(pngFileName.Data());
}
