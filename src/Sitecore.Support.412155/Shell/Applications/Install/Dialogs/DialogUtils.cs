using Sitecore.Jobs.AsyncUI;
using Sitecore.Exceptions;
using Sitecore.Web.UI.HtmlControls;
using Sitecore.Web.UI.Sheer;
using System.IO;

namespace Sitecore.Shell.Applications.Install.Dialogs
{
    using System;
    using Sitecore.Diagnostics;
    using Sitecore.Globalization;
    using Sitecore.IO;

    /// <summary></summary>
    internal class DialogUtils
    {
        /// <summary>Checks whether a directory for packages exists</summary>
        public static void CheckPackageFolder()
        {
            DirectoryInfo directoryInfo = new DirectoryInfo(ApplicationContext.PackagePath);
            bool isCurrentFolderExist = FileUtil.FolderExists(directoryInfo.FullName);
            bool isParentFolderExist = directoryInfo.Parent != null && FileUtil.FolderExists(directoryInfo.Parent.FullName);
            bool filePathHasInvalidChars = FileUtil.FilePathHasInvalidChars(ApplicationContext.PackagePath);

            if (isParentFolderExist && !filePathHasInvalidChars && !isCurrentFolderExist)
            {
                Directory.CreateDirectory(ApplicationContext.PackagePath);
                Log.Warn(string.Format("The '{0}' folder was not found and has been created. Please check your Sitecore configuration.", ApplicationContext.PackagePath), typeof(DialogUtils));
            }

            if (!Directory.Exists(ApplicationContext.PackagePath))
            {
                throw new ClientAlertException(string.Format(Translate.Text(Texts.CannotFindPathMessage), ApplicationContext.PackagePath));
            }
        }

        /// <summary></summary>
        public static void Browse(ClientPipelineArgs args, Edit fileEdit)
        {
            try
            {
                CheckPackageFolder();
                if (args.IsPostBack)
                {
                    if (!args.HasResult)
                    {
                        return;
                    }
                    if (fileEdit != null)
                    {
                        fileEdit.Value = args.Result;
                    }
                }
                else
                {
                    BrowseDialog.BrowseForOpen(ApplicationContext.PackagePath, "*.zip", Texts.ChoosePackage, Texts.ClickThePackageThatYouWantToInstallAndThenClickOpen, "People/16x16/box.png");
                    args.WaitForPostBack();
                }
            }
            catch (Exception exception)
            {
                Diagnostics.Log.Error("Failed to browse file", exception, typeof(DialogUtils));
                SheerResponse.Alert(exception.Message);
            }
        }

        /// <summary></summary>
        public static void Upload(ClientPipelineArgs args, Edit fileEdit)
        {
            try
            {
                CheckPackageFolder();
                if (!args.IsPostBack)
                {
                    UploadPackageForm.Show(ApplicationContext.PackagePath, true);
                    args.WaitForPostBack();
                }
                else
                {
                    if (args.Result.StartsWith("ok:", StringComparison.InvariantCulture))
                    {
                        string names = args.Result.Substring("ok:".Length);
                        string[] fileNames = names.Split('|');
                        if (fileNames.Length >= 1 && fileEdit != null)
                        {
                            fileEdit.Value = fileNames[0];
                        }
                    }
                }
            }
            catch (Exception exception)
            {
                Diagnostics.Log.Error("Failed to upload file: " + args.Result, exception, typeof(DialogUtils));
                SheerResponse.Alert(exception.Message);
            }
        }

        /// <summary></summary>
        public static JobMonitor AttachMonitor(JobMonitor monitor)
        {
            if (monitor == null)
            {
                if (Sitecore.Context.ClientPage.IsEvent)
                {
                    monitor = Sitecore.Context.ClientPage.FindControl("Monitor") as JobMonitor;
                }
                else
                {
                    monitor = new JobMonitor();
                    monitor.ID = "Monitor";
                    Sitecore.Context.ClientPage.Controls.Add(monitor);
                }
            }
            return monitor;
        }
    }
}