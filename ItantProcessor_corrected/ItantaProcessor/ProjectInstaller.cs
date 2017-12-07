using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace ItantProcessor
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : System.Configuration.Install.Installer
    {
        public ProjectInstaller()
        {
            InitializeComponent();
            this.BeforeUninstall += new InstallEventHandler(serviceProcessInstaller1_BeforeUninstall);
            this.AfterInstall += new InstallEventHandler(serviceProcessInstaller1_AfterInstall);

        }

        private void serviceProcessInstaller1_BeforeUninstall(object sender, InstallEventArgs e)
        {
            using (ServiceController sc = new ServiceController("ItantaProcessor"))
            {
                sc.Stop();
            }
        }

        private void serviceProcessInstaller1_AfterInstall(object sender, InstallEventArgs e)
        {
            using (ServiceController sc = new ServiceController("ItantaProcessor"))
            {
                sc.Start();
            }
        }

        private void serviceProcessInstaller1_AfterInstall_1(object sender, InstallEventArgs e)
        {

        }

        private void ItantaProcessor_AfterInstall(object sender, InstallEventArgs e)
        {

        }
    }
}
