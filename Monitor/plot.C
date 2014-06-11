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

//#include "MitPlots/Style/interface/MitStyle.h"

using namespace std;
//using namespace mithep;

void overlayFrame(TString text);

//--------------------------------------------------------------------------------------------------
void plot()
{
  TString fileName = gSystem->Getenv("MONITOR_FILE");

  TDatime date;
  TString dateTime = date.AsSQLString();
  TString pngFileName = fileName + TString(".png");
  TString inputFile = fileName;

  // Make sure we have the right styles
  //MitStyle::Init();
  //gStyle->SetPadTopMargin(0.055); // to make sure the exponent is on the picture

  // will execute a shell command to get the data

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
  xMaxTb = 1000.;
  xMax   = 1.2;

  TH1D *hTotal    = new TH1D("Total",   "Total Space",     1000./20,xMin,xMaxTb);
  TH1D *hUsed     = new TH1D("Used",    "Used Space",      1000./20,xMin,xMaxTb);
  TH1D *hToDelete = new TH1D("ToDelete","To Delete Space", 1000./20,xMin,xMaxTb);
  TH1D *hLastCp   = new TH1D("LastCp",  "Last CP Space",   1000./20,xMin,xMaxTb);
  TH1D *hLastCpFr = new TH1D("LastCpFr","Last CP fraction",20,      xMin,xMax);

  //MitStyle::InitHist(h,"","",kBlack);  
  TString titles;
  titles = TString("; Size [TB]; Number of Sites");
  hTotal   ->SetTitle(titles.Data());
  hUsed    ->SetTitle(titles.Data());
  hToDelete->SetTitle(titles.Data());
  hLastCp  ->SetTitle(titles.Data());
  titles = TString("; Last CP Filling Fraction; Number of Sites");
  hLastCpFr->SetTitle(titles.Data());

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
  plotText->SetTextSize(0.04);
  plotText->SetTextColor(kBlue);
  plotText->Draw();

  return;
}
