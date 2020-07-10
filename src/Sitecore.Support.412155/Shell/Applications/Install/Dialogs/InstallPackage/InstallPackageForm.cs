using System;
using System.IO;
using Sitecore.Jobs;
using Sitecore.Jobs.AsyncUI;
using Sitecore.Diagnostics;
using Sitecore.Globalization;
using Sitecore.Install;
using Sitecore.Install.Files;
using Sitecore.Install.Framework;
using Sitecore.Install.Metadata;
using Sitecore.Install.Zip;
using Sitecore.IO;
using Sitecore.Web;
using Sitecore.Web.UI.HtmlControls;
using Sitecore.Web.UI.Pages;
using Sitecore.Web.UI.Sheer;

namespace Sitecore.Shell.Applications.Install.Dialogs.InstallPackage
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Sitecore.Data;
    using Sitecore.Events;
    using Sitecore.Install.Events;

    /// <summary></summary>
    public partial class InstallPackageForm : WizardForm
    {
        #region controls

        /// <summary></summary>
        protected Edit PackageFile;
        /// <summary></summary>
        protected Edit PackageName;
        /// <summary></summary>
        protected Edit Version;
        /// <summary></summary>
        protected Edit Author;
        /// <summary></summary>
        protected Edit Publisher;
        /// <summary></summary>
        protected Border LicenseAgreement;
        /// <summary></summary>
        protected Memo ReadmeText;
        /// <summary></summary>
        protected Radiobutton Decline;
        /// <summary></summary>
        protected Radiobutton Accept;
        /// <summary></summary>
        protected Checkbox Restart;
        /// <summary></summary>
        protected Checkbox RestartServer;
        /// <summary></summary>
        protected JobMonitor Monitor;
        /// <summary></summary>
        protected Literal FailingReason;
        /// <summary></summary>
        protected Literal ErrorDescription;
        /// <summary></summary>
        protected Border SuccessMessage;
        /// <summary></summary>
        protected Border ErrorMessage;
        /// <summary></summary>
        protected Border AbortMessage;

        #endregion controls

        #region properties

        /// <summary>
        /// Gets or sets a value indicating whether this instance has license.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance has license; otherwise, <c>false</c>.
        /// </value>
        public bool HasLicense
        {
            get
            {
                return MainUtil.GetBool(Context.ClientPage.ServerProperties["HasLicense"], false);
            }
            set
            {
                Context.ClientPage.ServerProperties["HasLicense"] = value.ToString();
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether this instance has readme.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance has readme; otherwise, <c>false</c>.
        /// </value>
        public bool HasReadme
        {
            get
            {
                return MainUtil.GetBool(Context.ClientPage.ServerProperties["Readme"], false);
            }
            set
            {
                Context.ClientPage.ServerProperties["Readme"] = value.ToString();
            }
        }

        /// <summary>
        /// Gets or sets the post action.
        /// </summary>
        /// <value>The post action.</value>
        [NotNull]
        string PostAction
        {
            get
            {
                return StringUtil.GetString(ServerProperties["postAction"]);
            }
            set
            {
                ServerProperties["postAction"] = value;
            }
        }

        enum InstallationSteps
        {
            MainInstallation,
            WaitForFiles,
            InstallSecurity,
            RunPostAction,
            None,
            Failed
        }

        /// <summary>
        /// Synchronization object for current step
        /// </summary>
        readonly object CurrentStepSync = new object();

        /// <summary>
        /// Gets or sets the installation step. 0 means installing items and files, 1 means installing security accounts.
        /// </summary>
        /// <value>The installation step.</value>
        InstallationSteps CurrentStep
        {
            get
            {
                return (InstallationSteps)(int)ServerProperties["installationStep"];
            }
            set
            {
                lock (CurrentStepSync)
                {
                    ServerProperties["installationStep"] = (int)value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the package version.
        /// </summary>
        /// <value>The package version.</value>
        int PackageVersion
        {
            get
            {
                return int.Parse(StringUtil.GetString(ServerProperties["packageType"], "1"));
            }
            set
            {
                ServerProperties["packageType"] = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether this installation is successful.
        /// </summary>
        /// <value><c>true</c> if successful; otherwise, <c>false</c>.</value>
        bool Successful
        {
            get
            {
                object value = ServerProperties["Successful"];
                return value is bool ? (bool)value : true;
            }
            set
            {
                ServerProperties["Successful"] = value;
            }
        }

        /// <summary>
        /// Gets or sets the main installation task ID.
        /// </summary>
        /// <value>The main installation task ID.</value>
        string MainInstallationTaskID
        {
            get
            {
                return StringUtil.GetString(ServerProperties["taskID"]);
            }
            set
            {
                ServerProperties["taskID"] = value;
            }
        }


        #endregion properties

        #region overrides

        /// <summary></summary>
        protected override void OnLoad(EventArgs e)
        {
            if (!Context.ClientPage.IsEvent)
            {
                OriginalNextButtonHeader = NextButton.Header;
            }
            base.OnLoad(e);
            Monitor = DialogUtils.AttachMonitor(Monitor);
            if (!Context.ClientPage.IsEvent)
            {
                PackageFile.Value = Registry.GetString("Packager/File");
                Decline.Checked = true;
                Restart.Checked = true;
                RestartServer.Checked = false;
            }
            Monitor.JobFinished += Monitor_JobFinished;
            Monitor.JobDisappeared += Monitor_JobDisappeared;

            this.WizardCloseConfirmationText = Texts.AreYouSureYouWantToCancelInstallingAPackage;
        }

        /// <summary>
        /// Called when the active page is changing.
        /// </summary>
        /// <param name="page">The page that is being left.</param>
        /// <param name="newpage">The new page that is being entered.</param>
        protected override bool ActivePageChanging(string page, ref string newpage)
        {
            bool changingResult = base.ActivePageChanging(page, ref newpage);
            if ((page == "LoadPackage") && (newpage == "License"))
            {
                changingResult = LoadPackage();
                if (!HasLicense)
                {
                    newpage = "Readme";
                    if (!HasReadme)
                    {
                        newpage = "Ready";
                    }
                }
                return changingResult;
            }
            if ((page == "License") && (newpage == "Readme"))
            {
                if (!HasReadme)
                {
                    newpage = "Ready";
                }
                return changingResult;
            }
            if ((page == "Ready") && (newpage == "Readme"))
            {
                if (!HasReadme)
                {
                    newpage = "License";
                    if (!HasLicense)
                    {
                        newpage = "LoadPackage";
                    }
                }
                return changingResult;
            }
            if (((page == "Readme") && (newpage == "License")) && !HasLicense)
            {
                newpage = "LoadPackage";
            }

            return changingResult;
        }

        /// <summary>
        /// Called when the active page has been changed.
        /// </summary>
        /// <param name="page">The page that has been entered.</param>
        /// <param name="oldPage">The page that was left.</param>
        protected override void ActivePageChanged(string page, string oldPage)
        {
            base.ActivePageChanged(page, oldPage);

            NextButton.Header = OriginalNextButtonHeader;

            if ((page == "License") && (oldPage == "LoadPackage"))
            {
                NextButton.Disabled = !Accept.Checked;
            }
            if (page == "Installing")
            {
                BackButton.Disabled = true;
                NextButton.Disabled = true;
                CancelButton.Disabled = true;
                Context.ClientPage.SendMessage(this, "installer:startInstallation");
            }
            if (page == "Ready")
            {
                NextButton.Header = Translate.Text("Install");
            }
            if (page == "LastPage")
            {
                BackButton.Disabled = true;
            }

            if (!Successful)
            {
                CancelButton.Header = Translate.Text("Close");
                Successful = true;
            }
        }

        /// <summary></summary>
        protected override void EndWizard()
        {
            if (!Cancelling)
            {
                if (RestartServer.Checked)
                {
                    Installer.RestartServer();
                }
                if (Restart.Checked)
                {
                    // Shell is Sitecore Desktop's frame name
                    Context.ClientPage.ClientResponse.Broadcast(Context.ClientPage.ClientResponse.SetLocation(string.Empty), "Shell");
                }
            }
            Framework.Windows.Close();
        }

        /// <summary></summary>
        protected override void OnCancel(object sender, EventArgs formEventArgs)
        {
            Cancel();
        }

        /// <summary>
        /// On "Cancel" click
        /// </summary>
        public new void Cancel()
        {
            int index = Pages.IndexOf(Active);

            if (index == 0 || index == Pages.Count - 1)
            {
                Cancelling = index == 0;
                EndWizard();
            }
            else
            {
                Cancelling = true;
                Context.ClientPage.Start(this, "Confirmation");
            }
        }

        #endregion overrides

        #region message handlers

        /// <summary></summary>
        protected void Done()
        {
            Active = "LastPage";
            BackButton.Disabled = true;
            NextButton.Disabled = true;
            CancelButton.Disabled = false;
        }

        /// <summary>
        /// Starts the installation.
        /// </summary>
        /// <param name="message">The message.</param>
        [HandleMessage("installer:startInstallation")]
        protected void StartInstallation([NotNull] Message message)
        {
            Assert.ArgumentNotNull(message, "message");

            CurrentStep = InstallationSteps.MainInstallation;
            string packageFile = Installer.GetFilename(PackageFile.Value);
            if (FileUtil.IsFile(packageFile))
            {
                StartTask(packageFile);
            }
            else
            {
                Context.ClientPage.ClientResponse.Alert("Package not found");
                Active = "Ready";
                BackButton.Disabled = true;
            }
        }

        /// <summary>
        /// Sets the task ID.
        /// </summary>
        /// <param name="message">The message.</param>
        [HandleMessage("installer:setTaskId")]
        [UsedImplicitly]
        [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode", Justification = "The member is used implicitly.")]
        private void SetTaskID([NotNull] Message message)
        {
            Assert.ArgumentNotNull(message, "message");

            Assert.IsNotNull(message["id"], "id");
            MainInstallationTaskID = message["id"];
        }

        [HandleMessage("installer:commitingFiles")]
        [UsedImplicitly]
        [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode", Justification = "The member is used implicitly.")]
        private void OnCommittingFiles([NotNull] Message message)
        {
            Assert.ArgumentNotNull(message, "message");

            lock (CurrentStepSync)
            {
                if (CurrentStep == InstallationSteps.MainInstallation)
                {
                    CurrentStep = InstallationSteps.WaitForFiles;
                    WatchForInstallationStatus();
                }
            }
        }

        void Monitor_JobFinished([NotNull] object sender, [NotNull] EventArgs e)
        {
            Assert.ArgumentNotNull(sender, "sender");
            Assert.ArgumentNotNull(e, "e");

            lock (CurrentStepSync)
            {
                switch (CurrentStep)
                {
                    case InstallationSteps.MainInstallation:
                        CurrentStep = InstallationSteps.WaitForFiles;
                        WatchForInstallationStatus();
                        break;

                    case InstallationSteps.WaitForFiles:
                        CurrentStep = InstallationSteps.InstallSecurity;
                        StartInstallingSecurity();
                        break;

                    case InstallationSteps.InstallSecurity:
                        CurrentStep = InstallationSteps.RunPostAction;
                        if (string.IsNullOrEmpty(PostAction))
                        {
                            GotoLastPage(Result.Success, string.Empty, string.Empty);
                        }
                        else
                        {
                            StartPostAction();
                        }
                        break;

                    case InstallationSteps.RunPostAction:
                        GotoLastPage(Result.Success, string.Empty, string.Empty);
                        break;
                }
            }
        }

        void Monitor_JobDisappeared([NotNull] object sender, [NotNull] EventArgs e)
        {
            Assert.ArgumentNotNull(sender, "sender");
            Assert.ArgumentNotNull(e, "e");

            lock (CurrentStepSync)
            {
                switch (CurrentStep)
                {
                    case InstallationSteps.MainInstallation:
                        GotoLastPage(Result.Failure, Translate.Text(Texts.INSTALLATION_COULD_NOT_BE_COMPLETED),
                                     Translate.Text(Texts.INSTALLATION_JOB_WAS_INTERRUPTED_UNEXPECTEDLY));
                        break;

                    case InstallationSteps.WaitForFiles:
                        WatchForInstallationStatus();
                        break;

                    default:
                        Monitor_JobFinished(sender, e);
                        break;
                }
            }
        }

        /// <summary></summary>
        [HandleMessage("installer:browse", true)]
        protected void Browse(ClientPipelineArgs args)
        {
            DialogUtils.Browse(args, PackageFile);
        }

        /// <summary></summary>
        [HandleMessage("installer:upload", true)]
        protected void Upload(ClientPipelineArgs args)
        {
            DialogUtils.Upload(args, PackageFile);
        }

        /// <summary></summary>
        [HandleMessage("installer:savePostAction")]
        protected void SavePostAction(Message msg)
        {
            string action = msg.Arguments[0];
            PostAction = action;
        }

        /// <summary></summary>
        [HandleMessage("installer:doPostAction")]
        protected void DoPostAction(Message msg)
        {
            string action = PostAction;
            if (!string.IsNullOrEmpty(action))
            {
                StartPostAction();
            }
        }

        /// <summary></summary>
        [HandleMessage("installer:aborted")]
        protected void OnInstallerAborted(Message message)
        {
            GotoLastPage(Result.Abort, string.Empty, string.Empty);
            CurrentStep = InstallationSteps.Failed;
        }

        /// <summary></summary>
        [HandleMessage("installer:failed")]
        protected void OnInstallerFailed(Message message)
        {
            Job job = JobManager.GetJob(Monitor.JobHandle);
            Assert.IsNotNull(job, "Job is not available");

            var e = job.Status.Result as Exception;
            Error.AssertNotNull(e, "Cannot get any exception details");

            GotoLastPage(Result.Failure, GetShortDescription(e), GetFullDescription(e));
            CurrentStep = InstallationSteps.Failed;
        }

        enum Result
        {
            Success,
            Failure,
            Abort
        }

        void GotoLastPage(Result result, string shortDescription, string fullDescription)
        {
            ErrorDescription.Text = fullDescription;
            FailingReason.Text = shortDescription;


            Cancelling = result != Result.Success;

            SetVisibility(SuccessMessage, result == Result.Success);
            SetVisibility(ErrorMessage, result == Result.Failure);
            SetVisibility(AbortMessage, result == Result.Abort);
            var installationArgs = new InstallationEventArgs(new List<ItemUri>(), new List<FileCopyInfo>(), "packageinstall:ended");
            Event.RaiseEvent("packageinstall:ended", installationArgs);
            Successful = result == Result.Success;
            Active = "LastPage";
        }

        bool Cancelling
        {
            get
            {
                return MainUtil.GetBool(Context.ClientPage.ServerProperties["__cancelling"], false);
            }
            set
            {
                Context.ClientPage.ServerProperties["__cancelling"] = value;
            }
        }

        /// <summary></summary>
        protected void Agree()
        {
            NextButton.Disabled = false;
            Context.ClientPage.ClientResponse.SetReturnValue(true);
        }

        /// <summary></summary>
        protected void Disagree()
        {
            NextButton.Disabled = true;
            Context.ClientPage.ClientResponse.SetReturnValue(true);
        }

        /// <summary></summary>
        protected void RestartInstallation()
        {
            Active = "Ready";
            CancelButton.Visible = true;
            CancelButton.Disabled = false;
            NextButton.Visible = true;
            NextButton.Disabled = false;
            BackButton.Visible = false;
        }

        #endregion message handlers

        #region private scope

        static string GetFullDescription(Exception e)
        {
            // To do: add extended information such as sitecore and instraller vestions
            return e.ToString();
        }

        static string GetShortDescription(Exception e)
        {
            string message = e.Message;
            int pos = message.IndexOf("(method:", StringComparison.InvariantCulture);
            if (pos > -1)
            {
                return message.Substring(0, pos - 1);
            }
            return message;
        }

        static void SetVisibility(Control control, bool visible)
        {
            Context.ClientPage.ClientResponse.SetStyle(control.ID, "display", visible ? "" : "none");
        }

        string OriginalNextButtonHeader
        {
            get
            {
                return StringUtil.GetString(Context.ClientPage.ServerProperties["next-header"]);
            }
            set
            {
                Context.ClientPage.ServerProperties["next-header"] = value;
            }
        }

        bool LoadPackage()
        {
            string packageFile = PackageFile.Value;
            if (Path.GetExtension(packageFile).Trim().Length == 0)
            {
                packageFile = Path.ChangeExtension(packageFile, ".zip");
                PackageFile.Value = packageFile;
            }
            if (packageFile.Trim().Length == 0)
            {
                Context.ClientPage.ClientResponse.Alert("Please specify a package.");
                return false;
            }
            packageFile = Installer.GetFilename(packageFile);
            if (!FileUtil.FileExists(packageFile))
            {
                Context.ClientPage.ClientResponse.Alert(Translate.Text("The package \"{0}\" file does not exist.", new object[] { packageFile }));
                return false;
            }

            IProcessingContext context = Installer.CreatePreviewContext();
            ISource<PackageEntry> reader = new PackageReader(MainUtil.MapPath(packageFile));
            MetadataView view = new MetadataView(context);
            MetadataSink metaSink = new MetadataSink(view);
            metaSink.Initialize(context);

            reader.Populate(metaSink);
            if ((context == null) || (context.Data == null))
            {
                Context.ClientPage.ClientResponse.Alert(Translate.Text("The package \"{0}\" could not be loaded.\n\nThe file maybe corrupt.", new object[] { packageFile }));
                return false;
            }

            PackageVersion = context.Data.ContainsKey("installer-version") ? 2 : 1;  // Do we need this line?
            PackageName.Value = view.PackageName;
            Version.Value = view.Version;
            Author.Value = view.Author;
            Publisher.Value = view.Publisher;
            LicenseAgreement.InnerHtml = view.License;
            ReadmeText.Value = view.Readme;
            HasLicense = view.License.Length > 0;
            HasReadme = view.Readme.Length > 0;
            PostAction = view.PostStep;
            Registry.SetString("Packager/File", PackageFile.Value);
            return true;
        }

        #region task helpers

        void StartTask(string packageFile)
        {
            Monitor.Start("Install", "Install", new AsyncHelper(packageFile).Install);
        }


        void WatchForInstallationStatus()
        {
            string fileName = FileInstaller.GetStatusFileName(MainInstallationTaskID);
            Monitor.Start("WatchStatus", "Install", new AsyncHelper().SetStatusFile(fileName).WatchForStatus);
        }

        void StartInstallingSecurity()
        {
            var packageFile = Installer.GetFilename(PackageFile.Value);
            Monitor.Start("InstallSecurity", "Install", new AsyncHelper(packageFile).InstallSecurity);
        }

        void StartPostAction()
        {
            if (Monitor.JobHandle != Handle.Null)
            {
                Log.Info("Waiting for installation task completion", this);
                SheerResponse.Timer("installer:doPostAction", 100);
                return;
            }
            string postAction = PostAction;
            PostAction = string.Empty;
            if ((postAction.IndexOf("://", StringComparison.InvariantCulture) < 0) && (postAction.StartsWith("/", StringComparison.InvariantCulture)))
            {
                postAction = WebUtil.GetServerUrl() + postAction;
            }
            Monitor.Start("RunPostAction", "Install", new AsyncHelper(postAction, GetContextWithMetadata()).ExecutePostStep);
        }

        IProcessingContext GetContextWithMetadata()
        {
            string packageFile = Installer.GetFilename(PackageFile.Value);
            IProcessingContext context = Installer.CreatePreviewContext();
            ISource<PackageEntry> reader = new PackageReader(MainUtil.MapPath(packageFile));
            MetadataView view = new MetadataView(context);
            MetadataSink metaSink = new MetadataSink(view);
            metaSink.Initialize(context);
            reader.Populate(metaSink);
            return context;
        }

        #endregion task helpers

        #endregion private scope
    }
}