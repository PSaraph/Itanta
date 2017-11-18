using System;
using System.Configuration.Install;
using System.ServiceProcess;

namespace ItantProcessor
{
    partial class ProjectInstaller
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ServiceProcess.ServiceInstaller ItantaProcessor;
            this.serviceProcessInstaller1 = new System.ServiceProcess.ServiceProcessInstaller();
            ItantaProcessor = new System.ServiceProcess.ServiceInstaller();
            // 
            // ItantaProcessor
            // 
            ItantaProcessor.ServiceName = "ItantaProcessor";
            ItantaProcessor.StartType = System.ServiceProcess.ServiceStartMode.Automatic;
            ItantaProcessor.DelayedAutoStart = true;
            // 
            // serviceProcessInstaller1
            // 
            this.serviceProcessInstaller1.Account = System.ServiceProcess.ServiceAccount.LocalSystem;
            this.serviceProcessInstaller1.Password = null;
            this.serviceProcessInstaller1.Username = null;
            this.serviceProcessInstaller1.AfterInstall += new System.Configuration.Install.InstallEventHandler(this.serviceProcessInstaller1_AfterInstall_1);
            // 
            // ProjectInstaller
            // 
            this.Installers.AddRange(new System.Configuration.Install.Installer[] {
            this.serviceProcessInstaller1,
            ItantaProcessor});

        }
        #endregion

        private System.ServiceProcess.ServiceProcessInstaller serviceProcessInstaller1;
    }
}