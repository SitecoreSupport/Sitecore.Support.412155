using System;
using System.Threading;
using Sitecore.Data.Engines;
using Sitecore.Jobs.AsyncUI;
using Sitecore.Data.Proxies;
using Sitecore.Diagnostics;
using Sitecore.Install;
using Sitecore.Install.Files;
using Sitecore.Install.Framework;
using Sitecore.Install.Items;
using Sitecore.Install.Security;
using Sitecore.Install.Utils;

namespace Sitecore.Shell.Applications.Install.Dialogs.InstallPackage
{
    using Sitecore.Configuration;
    using Sitecore.Globalization;
    using Sitecore.IO;

    /// <summary></summary>
    public partial class InstallPackageForm
    {

        private class AsyncHelper
        {
            #region variables

            string _packageFile;
            string _postAction;
            IProcessingContext _context;
            StatusFile _statusFile;
            private Language _language;

            #endregion variables

            /// <summary>
            /// Initializes a new instance of the <see cref="AsyncHelper"/> class.
            /// </summary>
            /// <param name="package">The package.</param>
            public AsyncHelper(string package)
            {
                _packageFile = package;
                _language = Context.Language;
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="AsyncHelper"/> class.
            /// </summary>
            /// <param name="postAction">The post action.</param>
            /// <param name="context">The context.</param>
            public AsyncHelper(string postAction, IProcessingContext context)
            {
                _postAction = postAction;
                _context = context;
                _language = Context.Language;
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="AsyncHelper"/> class.
            /// </summary>
            public AsyncHelper()
            {
                _language = Context.Language;
            }

            /// <summary>
            /// Performs installation.
            /// </summary>
            public void Install()
            {
                CatchExceptions(delegate {
                    using (new SecurityModel.SecurityDisabler())
                    {
                        using (new SyncOperationContext())
                        {
                            using (new LanguageSwitcher(_language))
                            {
                                using (var drive = new VirtualDrive(FileUtil.MapPath(Settings.TempFolderPath)))
                                {
                                    SettingsSwitcher settingsSwitcher = null;
                                    try
                                    {
                                        if (!string.IsNullOrEmpty(drive.Name))
                                        {
                                            settingsSwitcher = new SettingsSwitcher("TempFolder", drive.Name);
                                        }

                                        var processingContext = Installer.CreateInstallationContext();
                                        JobContext.PostMessage("installer:setTaskId(id=" + processingContext.TaskID + ")");
                                        processingContext.AddAspect<IItemInstallerEvents>(new UiInstallerEvents());
                                        processingContext.AddAspect<IFileInstallerEvents>(new UiInstallerEvents());
                                        var installer = new Installer();
                                        installer.InstallPackage(PathUtils.MapPath(_packageFile), processingContext);
                                    }
                                    finally
                                    {
                                        if (settingsSwitcher != null)
                                        {
                                            settingsSwitcher.Dispose();
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }

            /// <summary>
            /// Installs the security.
            /// </summary>
            public void InstallSecurity()
            {
                CatchExceptions(delegate {
                    using (new LanguageSwitcher(_language))
                    {
                        var processingContext = Installer.CreateInstallationContext();
                        processingContext.AddAspect<IAccountInstallerEvents>(new UiInstallerEvents());
                        var installer = new Installer();
                        installer.InstallSecurity(PathUtils.MapPath(_packageFile), processingContext);
                    }
                });
            }

            /// <summary>
            /// Sets the status file.
            /// </summary>
            /// <param name="filename">The filename.</param>
            /// <returns>The status file.</returns>
            public AsyncHelper SetStatusFile(string filename)
            {
                _statusFile = new StatusFile(filename);
                return this;
            }

            /// <summary>
            /// Watches for status.
            /// </summary>
            /// <exception cref="Exception"><c>Exception</c>.</exception>
            public void WatchForStatus()
            {
                CatchExceptions(delegate {
                    Assert.IsNotNull(_statusFile, "Internal error: status file not set.");
                    bool ok = false;
                    do
                    {
                        StatusFile.StatusInfo info = _statusFile.ReadStatus();
                        if (info == null)
                        {
                            continue;
                        }
                        switch (info.Status)
                        {
                            case StatusFile.Status.Finished:
                                ok = true;
                                break;
                            case StatusFile.Status.Failed:
                                throw new Exception("Background process failed: " + info.Exception.Message, info.Exception);
                        }
                        Thread.Sleep(100);
                    }
                    while (!ok);

                });
            }

            /// <summary></summary>
            public void ExecutePostStep()
            {
                CatchExceptions(delegate {
                    var installer = new Installer();
                    installer.ExecutePostStep(_postAction, _context);
                });
            }

            private void CatchExceptions(ThreadStart start)
            {
                try
                {
                    start();
                }
                catch (ThreadAbortException)
                {
                    if (!Environment.HasShutdownStarted)
                    {
                        Thread.ResetAbort();
                    }
                    Log.Info("Installation was aborted", this);
                    JobContext.PostMessage("installer:aborted");
                    JobContext.Flush();
                }
                catch (Exception e)
                {
                    Log.Error("Installation failed: " + e, this);
                    JobContext.Job.Status.Result = e;
                    JobContext.PostMessage("installer:failed");
                    JobContext.Flush();
                }
            }
        }
    }
}