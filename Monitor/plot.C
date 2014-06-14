//==================================================================================================
//
// Plot the IntelROCCS logfile data.
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

void plotSiteStatus();
void plotDatasetUsage();
void overlayFrame(TString text);

//--------------------------------------------------------------------------------------------------
void plot()
{
  plotSiteStatus();
  plotDatasetUsage();
}

//--------------------------------------------------------------------------------------------------
void plotSiteStatus()
{
  TString  styleMacro = gSystem->Getenv("MIT_ROOT_STYLE");
  long int rc = gROOT->LoadMacro(styleMacro+"+");
  printf(" Return code of loading styles: %d\n",rc);

  TString fileName = gSystem->Getenv("SITE_MONITOR_FILE");

  TDatime date;
  TString dateTime = date.AsSQLString();
  TString pngFileName = fileName + TString(".png");
  TString inputFile = fileName;

  // Make sure we have the right styles
  MitRootStyle::Init();

  // Now open our database output
  printf(" Input file: %s\n",inputFile.Data());
  ifstream input;
  input.open(inputFile.Data());

  TString  siteName;
  Int_t    nLines=0;
  Double_t total=0, used=0, toDelete=0, lastCp=0;
  Double_t xMin=0, xMaxTb=0, xMax=0;

  // Loop over the file to determine our boundaries
  //-----------------------------------------------
  while (1) {

    // read in 
    input >> siteName >> total >> used >> toDelete >> lastCp;

    // check it worked
    if (! input.good())
      break;
    
    // show what we are reading
    if (nLines < 5)
      printf(" site=%s, total=%8f used=%f  toDelete=%f lastCp=%f\n",
	     siteName.Data(), total, used, toDelete, lastCp);
    if (used>xMax)
      xMax = used;
    
    nLines++;
  }
  input.close();

  printf(" \n");
  printf(" Found %d entries.\n",nLines);
  printf(" Found %.3f as maximum.\n",xMax);
  printf(" \n");

  // book our histogram
  xMaxTb = 999.;
  xMax   = 1.2;

  TString titles;
  titles = TString();

  TH1D *hTotal    = new TH1D("Total",   "Total Space",     1000./20,xMin,xMaxTb);
  MitRootStyle::InitHist(hTotal,"","",kBlack);
  hTotal->SetTitle("; Total Storage [TB]; Number of Sites");
  TH1D *hUsed     = new TH1D("Used",    "Used Space",      1000./20,xMin,xMaxTb);
  MitRootStyle::InitHist(hUsed,    "","",kBlack);
  hUsed    ->SetTitle("; Used Storage [TB]; Number of Sites");
  TH1D *hToDelete = new TH1D("ToDelete","Space to Release", 1000./20,xMin,xMaxTb);
  MitRootStyle::InitHist(hToDelete,"","",kBlack);
  hToDelete->SetTitle("; Space to Release [TB]; Number of Sites");
  TH1D *hLastCp   = new TH1D("LastCp",  "Last Copy space", 1000./20,xMin,xMaxTb);
  MitRootStyle::InitHist(hLastCp,  "","",kBlack);
  hLastCp  ->SetTitle("; Last Copy Size [TB]; Number of Sites");
  TH1D *hLastCpFr = new TH1D("LastCpFr","Last CP fraction",20,      xMin,xMax);
  MitRootStyle::InitHist(hLastCpFr,"","",kBlack);
  hLastCpFr->SetTitle("; Last Copy Filling Fraction; Number of Sites");

  input.open(inputFile.Data());
  while (1) {

    // read in 
    input >> siteName >> total >> used >> toDelete >> lastCp;

    // check it worked
    if (! input.good())
      break;
    
    hTotal   ->Fill(total);
    hUsed    ->Fill(used);
    hToDelete->Fill(toDelete);
    hLastCp  ->Fill(lastCp);
    hLastCpFr->Fill(lastCp/total);

    // print some warnings
    if (lastCp/total > 0.7) {
      printf(" WARNING - Last copy space too large: %3.0f%% at  %s\n",
	     lastCp/total*100.,siteName.Data());
    }


    nLines++;
  }
  input.close();

  // draw the plots
  TCanvas *cv = 0;

  cv = new TCanvas();
  cv->Draw();
  hTotal->Draw("hist");
  overlayFrame(TString("Date: ") + dateTime);
  cv->SaveAs("Total.png");

  cv = new TCanvas();
  cv->Draw();
  hUsed->Draw("hist");
  overlayFrame(TString("Date: ") + dateTime);
  cv->SaveAs("Used.png");

  cv = new TCanvas();
  cv->Draw();
  hToDelete->Draw("hist");
  overlayFrame(TString("Date: ") + dateTime);
  cv->SaveAs("ToDelete.png");

  cv = new TCanvas();
  cv->Draw();
  hLastCp->Draw("hist");
  overlayFrame(TString("Date: ") + dateTime);
  cv->SaveAs("LastCp.png");

  cv = new TCanvas();
  cv->Draw();
  hLastCpFr->Draw("hist");
  overlayFrame(TString("Date: ") + dateTime);
  cv->SaveAs("LastCpFraction.png");
}

//--------------------------------------------------------------------------------------------------
void plotDatasetUsage()
{
  TString text        = "All";
  TString fileName    = "DatasetMonitor";
  TString pngFileName = fileName + TString(".png");    
  TString inputFile   = gSystem->Getenv("DATASET_MONITOR_FILE");

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

  overlayFrame(TString("Overflow in last bin."));

  cv->SaveAs(pngFileName.Data());
}

//--------------------------------------------------------------------------------------------------
void overlayFrame(TString text)
{
  // Overlay a linear frame from user coordinates (0 - 1, 0 - 1) and put the frame text

  // create new transparent pad for the text
  TPad *transPad = new TPad("transPad","Transparent Pad",0,0,1,1);
  transPad->SetFillStyle(4000);
  transPad->Draw();
  transPad->cd();

  // overlay the text in a well defined frame
  TText *plotText = new TText(0.01,0.01,text.Data());
  plotText->SetTextSize(0.02);
  plotText->SetTextColor(kBlue);
  plotText->Draw();

  return;
}
